package drain3

import (
	"fmt"
	"strings"
)

type LogCluster struct {
	ClusterID         int64
	LogTemplateTokens []string
	Size              int64
}

func NewLogCluster(clusterID int64, logTemplateTokens []string) *LogCluster {
	return &LogCluster{
		ClusterID:         clusterID,
		LogTemplateTokens: logTemplateTokens,
		Size:              1,
	}
}

func (l *LogCluster) GetTemplate() string {
	return strings.Join(l.LogTemplateTokens, " ")
}

func (l *LogCluster) String() string {
	return fmt.Sprintf("ID=%-5d : size=%-10d: %s", l.ClusterID, l.Size, l.GetTemplate())
}
