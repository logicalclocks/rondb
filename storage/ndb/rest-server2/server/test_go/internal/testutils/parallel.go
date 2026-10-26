/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package testutils

import (
	"runtime"
	"testing"
)

// ParallelTB lets helpers that fail through testing.TB run inside
// b.RunParallel. FailNow (and so Fatal/Fatalf) may only be called from the
// benchmark goroutine; from a worker it would stop that worker unreported
// to the caller, which then waits for results that never come. ParallelTB
// instead logs the failure with Errorf, which is safe from any goroutine
// and marks the benchmark failed, and ends only the calling worker. The
// caller checks b.Failed() after RunParallel.
type ParallelTB struct {
	testing.TB
}

func (p ParallelTB) Fatalf(format string, args ...any) {
	p.TB.Helper()
	p.TB.Errorf(format, args...)
	runtime.Goexit()
}

func (p ParallelTB) Fatal(args ...any) {
	p.TB.Helper()
	p.TB.Error(args...)
	runtime.Goexit()
}

func (p ParallelTB) FailNow() {
	p.TB.Helper()
	p.TB.Error("FailNow in a RunParallel worker")
	runtime.Goexit()
}
