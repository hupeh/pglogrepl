package pglogrepl

const (
	StatusStopped   int32 = iota // 已停止
	StatusStarting               // 正在启动中
	StatusSyncing                // 正在同步中
	StatusListening              // 正在监听中
	StatusStopping               // 正在停止中
)

func StatusName(s int32) string {
	switch s {
	case StatusStopped:
		return "stopped"
	case StatusStarting:
		return "starting"
	case StatusSyncing:
		return "syncing"
	case StatusListening:
		return "listening"
	case StatusStopping:
		return "stopping"
	default:
		return "unknown"
	}
}
