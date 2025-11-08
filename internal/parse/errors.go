package parse

import "fmt"

type NotInListError struct {
	item string
}

func (e *NotInListError) Error() string {
	return fmt.Sprintf("%s is not in list", e.item)
}
