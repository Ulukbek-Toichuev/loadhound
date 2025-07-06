/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"reflect"
	"sync"
	"testing"
)

func TestGetTopErrors(t *testing.T) {
	var m sync.Map
	m.Store("error A", 10)
	m.Store("error B", 5)
	m.Store("error C", 8)
	m.Store("error D", 15)
	m.Store("error E", 3)
	m.Store("error F", 20)

	expected := []string{"error F", "error D", "error A", "error C", "error B"}

	got := getTopErrors(&m)

	if !reflect.DeepEqual(got, expected) {
		t.Errorf("Expected %v, got %v", expected, got)
	}
}

func TestGetTopErrorsLessThanFive(t *testing.T) {
	var m sync.Map
	m.Store("error X", 7)
	m.Store("error Y", 2)

	expected := []string{"error X", "error Y"}

	got := getTopErrors(&m)

	if !reflect.DeepEqual(got, expected) {
		t.Errorf("Expected %v, got %v", expected, got)
	}
}
