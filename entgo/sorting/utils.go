package sorting

import (
	"strings"
	"unicode"

	"entgo.io/ent/dialect/sql"
)

// buildOrderBySelector 构建字段选择器
func buildOrderBySelector(s *sql.Selector, field string, desc bool) {
	field = normalizeOrderField(field)
	if field == "" {
		return
	}
	if desc {
		s.OrderBy(sql.Desc(s.C(field)))
	} else {
		s.OrderBy(sql.Asc(s.C(field)))
	}
}

func normalizeOrderField(field string) string {
	field = strings.TrimSpace(field)
	if field == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(field) + 4)
	for i, r := range field {
		switch {
		case r == '.' || r == '-' || unicode.IsSpace(r):
			builder.WriteByte('_')
		case unicode.IsUpper(r):
			if i > 0 {
				prev := rune(field[i-1])
				if prev != '_' && prev != '.' && prev != '-' && !unicode.IsSpace(prev) {
					builder.WriteByte('_')
				}
			}
			builder.WriteRune(unicode.ToLower(r))
		default:
			builder.WriteRune(unicode.ToLower(r))
		}
	}

	return builder.String()
}
