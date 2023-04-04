/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
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

package ping

import (
	"testing"
	"time"
)

/*
	go test \
		-test.bench BenchmarkSimple \
		-test.run=thisexpressionwontmatchanytest \
		-cpu 1,2,4,8 \
		-benchmem \
		-benchtime=100x \ 		// 100 times
		-benchtime=10s \ 		// 10 sec
		./internal/integrationtests/ping/
*/
func BenchmarkSimple(b *testing.B) {
	// Number of total operations
	numOps := b.N
	b.Logf("numOps: %d", numOps)

	runAgainstGrpcServer := false

	threadId := 0

	b.ResetTimer()
	start := time.Now()

	/*
		With 10-core CPU, this will run 10 Go-routines.
		These 10 Go-routines will split up b.N requests
		amongst each other. RunParallel() will only be
		run 10 times then (in contrast to bp.Next()).
	*/
	b.RunParallel(func(bp *testing.PB) {
		threadId++
		b.Logf("threadId: %d", threadId)

		/*
			Given 10 go-routines and b.N==50, each go-routine
			will run this 5 times.
		*/
		for bp.Next() {
			if runAgainstGrpcServer {
				sendGrpcPingRequest(b)
			} else {
				sendRestPingRequest(b)
			}
		}
	})
	b.StopTimer()

	opsPerSecond := float64(numOps) / time.Since(start).Seconds()
	nanoSecondsPerOp := float64(time.Since(start).Nanoseconds()) / float64(numOps)
	b.Logf("Throughput: %f operations/second", opsPerSecond)
	b.Logf("Latency: 	%f nanoseconds/operation", nanoSecondsPerOp)
}
