package filerotate

// Exported for testing

var RunOutdatedFileCloser = runOutdatedFileCloser

type FileManager = fileManager

func (m *fileManager) FilePath() string { return m.filePath }

func init() {
	startDefaultOutdatedFileCloserOnce = func() {}
}
