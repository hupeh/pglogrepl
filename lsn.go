package pglogrepl

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/jackc/pglogrepl"
)

var errLSN = errors.New("LSN")

// LSN is a PostgreSQL Log Sequence Number.
// See https://www.postgresql.org/docs/current/datatype-pg-lsn.html.
type LSN struct {
	filename string
	value    atomic.Uint64
	count    atomic.Uint32
	checksum uint32
}

// Valid 判断增量同步位置是否有效
func (l *LSN) Valid() bool {
	return l.value.Load() > 0
}

// Set 设置增量同步位置
func (l *LSN) Set(lsn pglogrepl.LSN) error {
	// 理论上 lsn 不能小于 l.value，但是由于我们对逻辑复制没有采用并发处理，
	// 所以这里只做不相等校验
	if l.value.Swap(uint64(lsn)) != uint64(lsn) {
		if l.count.Add(1) >= 1000 {
			return l.Persist()
		}
	}
	return nil
}

// Get 获取增量同步位置
func (l *LSN) Get() pglogrepl.LSN {
	return pglogrepl.LSN(l.value.Load())
}

// Reload 从文件中重新加载增量同步位置
func (l *LSN) Reload() error {
	file, err := os.Open(l.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to reload LSN: %w", err)
	}
	defer file.Close()

	var checksum uint32
	var str string
	n, err := fmt.Fscanf(file, "%d/%s", &checksum, &str)
	if err != nil {
		return fmt.Errorf("failed to scan %w file: %w", errLSN, err)
	}
	if n != 2 {
		if n > 0 {
			return fmt.Errorf("failed to scan %w file", errLSN)
		}
		l.value.Store(0)
		l.count.Store(0)
		return nil
	}
	if checksum != l.checksum {
		return fmt.Errorf(
			"failed to scan %w file: expected checksum %x, got %x",
			errLSN, l.checksum, checksum,
		)
	}
	val, err := pglogrepl.ParseLSN(str)
	if err != nil {
		return fmt.Errorf("failed to scan %w file: %w", errLSN, err)
	}

	l.value.Store(uint64(val))
	l.count.Store(0)

	return nil
}

// Reset 重置增量同步位置
func (l *LSN) Reset() error {
	l.value.Store(0)
	return l.Persist()
}

// Persist 持久化增量同步位置到文件
func (l *LSN) Persist() error {
	if l.count.Swap(0) == 0 {
		return nil
	}
	str := fmt.Sprintf("%d/%s", l.checksum, l.Get().String())
	return os.WriteFile(l.filename, []byte(str), 0644)
}

// String 增量同步位置的字符串表示
func (l *LSN) String() string {
	return l.Get().String()
}
