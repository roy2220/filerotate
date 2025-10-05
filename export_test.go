package filerotate

// Exported for testing

var CloseOutdatedFiles = closeOutdatedFiles

type FileManager = fileManager

func (m *fileManager) FilePath() string { return m.filePath }
