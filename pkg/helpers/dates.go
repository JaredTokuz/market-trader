package helpers

import "time"

func Bod(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}

func NextDay(t time.Time) time.Time {
	return t.AddDate(0,0,1)
}