package main

import "../mr"
import "unicode"
import "strings"
import "strconv"

// Map comments
func Map(filename string, contents string) []mr.KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter }

	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva.append(kva, kv)
	}
	return kva
}

// Reduce comments
func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
