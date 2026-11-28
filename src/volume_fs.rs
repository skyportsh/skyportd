//! File access inside server volumes.
//!
//! The daemon runs as root while the content of a volume is controlled by the
//! server owner, so a symlink planted there must never redirect a read or a
//! write to the host. Every path component below the volume root is opened with
//! `openat` and `O_NOFOLLOW`, which also leaves no window between a check and
//! the access.

use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path};

#[derive(Clone, Copy, Debug, Default)]
pub struct OpenMode {
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub create: bool,
    pub truncate: bool,
    /// Creates the missing directories leading to the file.
    pub create_parents: bool,
}

impl OpenMode {
    pub fn read() -> Self {
        Self {
            read: true,
            ..Self::default()
        }
    }

    pub fn overwrite() -> Self {
        Self {
            write: true,
            create: true,
            truncate: true,
            ..Self::default()
        }
    }

    pub fn append() -> Self {
        Self {
            append: true,
            create: true,
            ..Self::default()
        }
    }
}

/// Opens the regular file at `relative` below `root`, refusing symlinks.
/// `root` itself is chosen by the daemon and therefore trusted.
pub fn open(root: &Path, relative: &Path, mode: OpenMode) -> io::Result<File> {
    let mut names = Vec::new();

    for component in relative.components() {
        match component {
            Component::Normal(name) => names.push(c_string(name.as_bytes())?),
            Component::CurDir => {}
            _ => return Err(invalid_input("path must stay within the server volume")),
        }
    }

    let file_name = names
        .pop()
        .ok_or_else(|| invalid_input("path does not name a file"))?;

    let root = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(root)?;
    let mut directory = OwnedFd::from(root);

    for name in names {
        if mode.create_parents {
            // SAFETY: `directory` is an open descriptor and `name` is NUL terminated.
            let created = unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o755) };

            if created != 0 {
                let error = io::Error::last_os_error();

                if error.kind() != io::ErrorKind::AlreadyExists {
                    return Err(error);
                }
            }
        }

        directory = open_at(&directory, &name, libc::O_RDONLY | libc::O_DIRECTORY)?;
    }

    let writable = mode.write || mode.append;
    let mut flags = match (mode.read, writable) {
        (true, true) => libc::O_RDWR,
        (false, true) => libc::O_WRONLY,
        _ => libc::O_RDONLY,
    };

    if mode.append {
        flags |= libc::O_APPEND;
    }
    if mode.create {
        flags |= libc::O_CREAT;
    }

    // O_NONBLOCK keeps a planted FIFO from blocking the daemon.
    let file = File::from(open_at(&directory, &file_name, flags | libc::O_NONBLOCK)?);

    if !file.metadata()?.is_file() {
        return Err(invalid_input("path is not a regular file"));
    }

    // Truncating only after the type check leaves anything unexpected untouched.
    if mode.truncate && writable {
        file.set_len(0)?;
    }

    Ok(file)
}

fn open_at(directory: &OwnedFd, name: &CString, flags: libc::c_int) -> io::Result<OwnedFd> {
    let permissions: libc::c_uint = 0o644;
    // SAFETY: `directory` is an open descriptor and `name` is NUL terminated.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            permissions,
        )
    };

    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `openat` just returned this descriptor and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}

fn c_string(bytes: &[u8]) -> io::Result<CString> {
    CString::new(bytes).map_err(|_| invalid_input("path contains a NUL byte"))
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use tempfile::tempdir;

    #[test]
    fn writes_and_creates_parents_inside_the_volume() {
        let volume = tempdir().unwrap();
        let mode = OpenMode {
            create_parents: true,
            ..OpenMode::overwrite()
        };

        open(volume.path(), Path::new("a/b/file.txt"), mode)
            .unwrap()
            .write_all(b"content")
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(volume.path().join("a/b/file.txt")).unwrap(),
            "content"
        );
    }

    #[test]
    fn refuses_a_symlinked_file_and_leaves_its_target_alone() {
        let volume = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let target = outside.path().join("secret");
        std::fs::write(&target, "secret").unwrap();
        symlink(&target, volume.path().join("eula.txt")).unwrap();

        for mode in [OpenMode::read(), OpenMode::overwrite(), OpenMode::append()] {
            open(volume.path(), Path::new("eula.txt"), mode).expect_err("symlink must be refused");
        }

        assert_eq!(std::fs::read_to_string(&target).unwrap(), "secret");
    }

    #[test]
    fn refuses_a_dangling_symlink_instead_of_creating_its_target() {
        let volume = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let target = outside.path().join("created");
        symlink(&target, volume.path().join("link")).unwrap();

        open(volume.path(), Path::new("link"), OpenMode::overwrite())
            .expect_err("symlink must be refused");

        assert!(!target.exists());
    }

    #[test]
    fn refuses_a_symlinked_directory() {
        let volume = tempdir().unwrap();
        let outside = tempdir().unwrap();
        symlink(outside.path(), volume.path().join("config")).unwrap();
        let mode = OpenMode {
            create_parents: true,
            ..OpenMode::overwrite()
        };

        open(volume.path(), Path::new("config/file"), mode).expect_err("symlink must be refused");

        assert!(!outside.path().join("file").exists());
    }

    #[test]
    fn refuses_to_leave_the_volume() {
        let volume = tempdir().unwrap();

        open(volume.path(), Path::new("../file"), OpenMode::overwrite())
            .expect_err("traversal must be refused");
    }
}
