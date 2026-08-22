package logger

import(
	"os"

	"github.com/rs/zerolog"
)

//zerolog logger which logs with timestamps
func New() zerolog.Logger {
	return zerolog.New(os.Stdout).With().Timestamp().Logger()
}