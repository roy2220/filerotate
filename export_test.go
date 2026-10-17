package filerotate

func init() {
	startDefaultOutdatedFileCloserOnce = func() {}
}

// Exported for testing

var RunOutdatedFileCloser = runOutdatedFileCloser

type FileManager = fileManager

func (m *fileManager) FilePath() string { return m.filePath }
