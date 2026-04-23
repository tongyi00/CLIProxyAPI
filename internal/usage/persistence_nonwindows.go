//go:build !windows

package usage

import "os"

// defaultReplaceSnapshotFile 在 POSIX 系统上直接使用 rename，
// 在相同文件系统上 rename 具备原子性，是最简单的"要么旧要么新"替换方案。
func defaultReplaceSnapshotFile(from, to string) error {
	return os.Rename(from, to)
}

// defaultSyncSnapshotParentDir 在 POSIX 上需要 fsync 父目录，
// 否则 rename 产生的目录项可能只存在于页缓存，系统断电后会丢失。
func defaultSyncSnapshotParentDir(dir string) error {
	dirHandle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirHandle.Close()
	return dirHandle.Sync()
}
