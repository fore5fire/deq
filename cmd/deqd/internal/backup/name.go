package backup

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func nameForVersion(v uint64) string {
	return fmt.Sprintf("%016x%s", v, backupExtension)
}

func versionForName(name string) (uint64, error) {
	trimmed := strings.TrimSuffix(name, backupExtension)
	if trimmed == name {
		return 0, errors.New("incorrect extension")
	}
	return strconv.ParseUint(trimmed, 16, 64)
}
