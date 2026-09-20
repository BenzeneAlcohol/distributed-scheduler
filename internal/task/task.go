package task

import "strings"

// Name identifies a task understood by the scheduler and workers.
type Name string

const (
	GenerateReport   Name = "generate-report"
	SendEmail        Name = "send-email"
	SendNotification Name = "send-notification"
)

// Parse validates an externally supplied task name.
func Parse(value string) (Name, bool) {
	name := Name(strings.TrimSpace(value))
	switch name {
	case GenerateReport, SendEmail, SendNotification:
		return name, true
	default:
		return "", false
	}
}
