//go:build windows

package usage

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// defaultReplaceSnapshotFile 在 Windows 上使用 MoveFileEx 实现原子替换。
// MOVEFILE_REPLACE_EXISTING 允许覆盖已有文件；MOVEFILE_WRITE_THROUGH 要求立即落盘，
// 避免断电时目标文件处于仅在缓存中的中间状态。
func defaultReplaceSnapshotFile(from, to string) error {
	fromPtr, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return fmt.Errorf("encode source path for windows replace: %w", err)
	}
	toPtr, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return fmt.Errorf("encode destination path for windows replace: %w", err)
	}
	flags := uint32(windows.MOVEFILE_REPLACE_EXISTING | windows.MOVEFILE_WRITE_THROUGH)
	if err := windows.MoveFileEx(fromPtr, toPtr, flags); err != nil {
		return err
	}
	return nil
}

// defaultSyncSnapshotParentDir 在 Windows 上为 no-op：
// NTFS 不像 POSIX 需要显式 fsync 目录才能保证目录项落盘。
func defaultSyncSnapshotParentDir(string) error {
	return nil
}
