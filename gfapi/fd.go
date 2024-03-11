package gfapi

// This file includes lower level operations on fd like the ones in the 'syscall' package

// #cgo pkg-config: glusterfs-api
// #include "glusterfs/api/glfs.h"
// #include <stdlib.h>
// #include <time.h>
// #include <sys/stat.h>
import "C"
import (
	"os"
	"syscall"
	"time"
	"unsafe"
)

// Fd is the glusterfs fd type
type Fd struct {
	fd *C.glfs_fd_t
}

var _zero uintptr

// Fchmod changes the mode of the Fd to the given mode
//
// # Returns error on failure
//
// int glfs_chmod(glfs_t *fs, const char *path, mode_t mode)
// __THROW GFAPI_PUBLIC(glfs_chmod, 3.4.0);
func (fd *Fd) Fchmod(mode uint32) error {
	_, err := C.glfs_fchmod(fd.fd, C.mode_t(mode))

	return err
}

// Fchown changes the uid and gid of the Fd
//
// # Returns error on failure
//
// int glfs_fchown(glfs_fd_t *fd, uid_t uid, gid_t gid)
// __THROW GFAPI_PUBLIC(glfs_fchown, 3.4.0);
func (fd *Fd) Fchown(uid, gid uint32) error {
	_, err := C.glfs_fchown(fd.fd, C.uid_t(uid), C.gid_t(gid))

	return err
}

// Futimens changes the atime and mtime of the Fd
//
// # Returns error on failure
//
// int glfs_futimens(glfs_fd_t *fd, const struct timespec times[2])
// __THROW GFAPI_PUBLIC(glfs_futimens, 3.4.0);
func (fd *Fd) Futimens(atime, mtime time.Time) error {
	var amtime [2]C.struct_timespec
	amtime[0] = C.struct_timespec{tv_sec: C.long(atime.Unix()), tv_nsec: C.long(atime.Nanosecond())}
	amtime[1] = C.struct_timespec{tv_sec: C.long(mtime.Unix()), tv_nsec: C.long(mtime.Nanosecond())}

	_, err := C.glfs_futimens(fd.fd, &amtime[0])

	return err
}

// Fstat performs an fstat call on the Fd and saves stat details in the passed stat structure
//
// # Returns error on failure
//
// int glfs_fstat(glfs_fd_t *fd, struct stat *buf)
// __THROW GFAPI_PUBLIC(glfs_fstat, 3.4.0);
func (fd *Fd) Fstat(stat *syscall.Stat_t) error {

	ret, err := C.glfs_fstat(fd.fd, (*C.struct_stat)(unsafe.Pointer(stat)))
	if int(ret) < 0 {
		return err
	}
	return nil
}

// Fsync performs an fsync on the Fd
//
// # Returns error on failure
//
// int glfs_fsync(glfs_fd_t *fd, struct glfs_stat *prestat,struct glfs_stat *poststat)
// __THROW GFAPI_PUBLIC(glfs_fsync, 6.0);
func (fd *Fd) Fsync() error {
	ret, err := C.glfs_fsync(fd.fd, nil, nil)
	if ret < 0 {
		return err
	}
	return nil
}

// Ftruncate truncates the size of the Fd to the given size
//
// # Returns error on failure
//
// int glfs_ftruncate(glfs_fd_t *fd, off_t length, struct glfs_stat *prestat,struct glfs_stat *poststat)
// __THROW GFAPI_PUBLIC(glfs_ftruncate, 6.0);
func (fd *Fd) Ftruncate(size int64) error {
	_, err := C.glfs_ftruncate(fd.fd, C.off_t(size), nil, nil)

	return err
}

// Pread reads at most len(b) bytes into b from offset off in Fd
//
// # Returns number of bytes read on success and error on failure
//
// ssize_t glfs_pread(glfs_fd_t *fd, void *buf, size_t count, off_t offset, int flags, struct glfs_stat *poststat)
// __THROW GFAPI_PUBLIC(glfs_pread, 6.0);
func (fd *Fd) Pread(b []byte, off int64) (int, error) {
	n, err := C.glfs_pread(fd.fd, unsafe.Pointer(&b[0]), C.size_t(len(b)), C.off_t(off), 0, nil)

	return int(n), err
}

// Pwrite writes len(b) bytes from b into the Fd from offset off
//
// # Returns number of bytes written on success and error on failure
//
// ssize_t glfs_pwrite(glfs_fd_t *fd, const void *buf, size_t count, off_t offset, int flags, struct glfs_stat *prestat, struct glfs_stat *poststat)
// __THROW GFAPI_PUBLIC(glfs_pwrite, 6.0);
func (fd *Fd) Pwrite(b []byte, off int64) (int, error) {
	n, err := C.glfs_pwrite(fd.fd, unsafe.Pointer(&b[0]), C.size_t(len(b)), C.off_t(off), 0, nil, nil)

	return int(n), err
}

// Read reads at most len(b) bytes into b from Fd
//
// # Returns number of bytes read on success and error on failure
//
// ssize_t glfs_read(glfs_fd_t *fd, void *buf, size_t count, int flags)
// __THROW GFAPI_PUBLIC(glfs_read, 3.4.0);
func (fd *Fd) Read(b []byte) (n int, err error) {
	var p0 unsafe.Pointer

	if len(b) > 0 {
		p0 = unsafe.Pointer(&b[0])
	} else {
		p0 = unsafe.Pointer(&_zero)
	}

	// glfs_read returns a ssize_t. The value of which is the number of bytes written.
	// Unless, ret is -1, an error, implying to check errno. cgo collects errno as the
	// functions error return value.
	ret, e1 := C.glfs_read(fd.fd, p0, C.size_t(len(b)), 0)
	n = int(ret)
	if n < 0 {
		err = e1
	}

	return n, err
}

// Write writes len(b) bytes from b into the Fd
//
// # Returns number of bytes written on success and error on failure
//
// ssize_t glfs_write(glfs_fd_t *fd, const void *buf, size_t count, int flags)
// __THROW GFAPI_PUBLIC(glfs_write, 3.4.0);
func (fd *Fd) Write(b []byte) (n int, err error) {
	var p0 unsafe.Pointer

	if len(b) > 0 {
		p0 = unsafe.Pointer(&b[0])
	} else {
		p0 = unsafe.Pointer(&_zero)
	}

	// glfs_write returns a ssize_t. The value of which is the number of bytes written.
	// Unless, ret is -1, an error, implying to check errno. cgo collects errno as the
	// functions error return value.
	ret, e1 := C.glfs_write(fd.fd, p0, C.size_t(len(b)), 0)
	n = int(ret)
	if n < 0 {
		err = e1
	}

	return n, err
}

// off_t glfs_lseek(glfs_fd_t *fd, off_t offset, int whence)
// __THROW GFAPI_PUBLIC(glfs_lseek, 3.4.0);
func (fd *Fd) lseek(offset int64, whence int) (int64, error) {
	ret, err := C.glfs_lseek(fd.fd, C.off_t(offset), C.int(whence))

	return int64(ret), err
}

// int glfs_fallocate(glfs_fd_t *fd, int keep_size, off_t offset, size_t len)
// __THROW GFAPI_PUBLIC(glfs_fallocate, 3.5.0);
func (fd *Fd) Fallocate(mode int, offset int64, len int64) error {
	ret, err := C.glfs_fallocate(fd.fd, C.int(mode), C.off_t(offset), C.size_t(len))

	if ret == 0 {
		err = nil
	}
	return err
}

// ssize_t glfs_fgetxattr(glfs_fd_t *fd, const char *name, void *value, size_t size)
// __THROW GFAPI_PUBLIC(glfs_fgetxattr, 3.4.0);
func (fd *Fd) Fgetxattr(attr string, dest []byte) (int64, error) {
	var ret C.ssize_t
	var err error

	cattr := C.CString(attr)
	defer C.free(unsafe.Pointer(cattr))

	if len(dest) <= 0 {
		ret, err = C.glfs_fgetxattr(fd.fd, cattr, nil, 0)
	} else {
		ret, err = C.glfs_fgetxattr(fd.fd, cattr, unsafe.Pointer(&dest[0]), C.size_t(len(dest)))
	}

	if ret >= 0 {
		return int64(ret), nil
	} else {
		return int64(ret), err
	}
}

// int glfs_fsetattr(struct glfs_fd *glfd, struct glfs_stat *stat)
// __THROW GFAPI_PUBLIC(glfs_fsetattr, 6.0);
func (fd *Fd) Fsetxattr(attr string, data []byte, flags int) error {
	cattr := C.CString(attr)
	defer C.free(unsafe.Pointer(cattr))

	ret, err := C.glfs_fsetxattr(fd.fd, cattr, unsafe.Pointer(&data[0]), C.size_t(len(data)), C.int(flags))

	if ret == 0 {
		err = nil
	}
	return err
}

// int glfs_fremovexattr(glfs_fd_t *fd, const char *name)
// __THROW GFAPI_PUBLIC(glfs_fremovexattr, 3.4.0);
func (fd *Fd) Fremovexattr(attr string) error {
	cattr := C.CString(attr)
	defer C.free(unsafe.Pointer(cattr))

	ret, err := C.glfs_fremovexattr(fd.fd, cattr)

	if ret == 0 {
		err = nil
	}
	return err
}

func direntName(dirent *syscall.Dirent) string {
	name := make([]byte, 0, len(dirent.Name))
	for i, c := range dirent.Name {
		if c == 0 || i > 255 {
			break
		}

		name = append(name, byte(c))
	}

	return string(name)
}

// Readdir returns the information of files in a directory.
//
// n is the maximum number of items to return. If there are more items than
// the maximum they can be obtained in successive calls. If maximum is 0
// then all the items will be returned.
//
// struct dirent *glfs_readdirplus(glfs_fd_t *fd, struct stat *stat)
// __THROW GFAPI_PUBLIC(glfs_readdirplus, 3.5.0);
func (fd *Fd) Readdir(n int) ([]os.FileInfo, error) {
	var (
		stat  syscall.Stat_t
		files []os.FileInfo
		statP = (*C.struct_stat)(unsafe.Pointer(&stat))
	)

	for i := 0; n == 0 || i < n; i++ {
		d, err := C.glfs_readdirplus(fd.fd, statP)
		if err != nil {
			return nil, err
		}

		dirent := (*syscall.Dirent)(unsafe.Pointer(d))
		if dirent == nil {
			break
		}

		name := direntName(dirent)
		file := fileInfoFromStat(&stat, name)
		files = append(files, file)
	}

	return files, nil
}

// Readdirnames returns the names of files in a directory.
//
// n is the maximum number of items to return and works the same way as Readdir.
//
// struct dirent *glfs_readdir(glfs_fd_t *fd)
// __THROW GFAPI_PUBLIC(glfs_readdir, 3.5.0);
func (fd *Fd) Readdirnames(n int) ([]string, error) {
	var names []string

	for i := 0; n == 0 || i < n; i++ {
		d, err := C.glfs_readdir(fd.fd)
		if err != nil {
			return nil, err
		}

		dirent := (*syscall.Dirent)(unsafe.Pointer(d))
		if dirent == nil {
			break
		}

		name := direntName(dirent)
		names = append(names, name)
	}

	return names, nil
}
