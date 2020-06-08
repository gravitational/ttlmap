/*
Copyright 2017 Mailgun Technologies Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package ttlmap

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/suite"
)

type TTLMapSuite struct {
	suite.Suite
	clock clockwork.Clock
}

func TestTTLMapSuite(t *testing.T) {
	suite.Run(t, new(TTLMapSuite))
}

func (s *TTLMapSuite) TestSetWrong() {
	m := NewTTLMap(1)

	err := m.Set("a", 1, -1)
	s.Require().EqualError(err, "ttlSeconds should be >= 0, got -1")

	err = m.Set("a", 1, 0)
	s.Require().EqualError(err, "ttlSeconds should be >= 0, got 0")

	_, err = m.Increment("a", 1, 0)
	s.Require().EqualError(err, "ttlSeconds should be >= 0, got 0")

	_, err = m.Increment("a", 1, -1)
	s.Require().EqualError(err, "ttlSeconds should be >= 0, got -1")
}

func (s *TTLMapSuite) TestRemoveExpiredEmpty() {
	m := NewTTLMap(1)
	m.RemoveExpired(100)
}

func (s *TTLMapSuite) TestRemoveLastUsedEmpty() {
	m := NewTTLMap(1)
	m.RemoveLastUsed(100)
}

func (s *TTLMapSuite) TestGetSetExpire() {
	clock := clockwork.NewFakeClock()
	m := newTTLMap(1, clock)

	err := m.Set("a", 1, 1)
	s.Require().Equal(nil, err)

	valI, exists := m.Get("a")
	s.Require().Equal(true, exists)
	s.Require().Equal(1, valI)

	clock.Advance(1 * time.Second)

	_, exists = m.Get("a")
	s.Require().Equal(false, exists)
}

func (s *TTLMapSuite) TestSetOverwrite() {
	m := NewTTLMap(1)

	err := m.Set("o", 1, 1)
	s.Require().Equal(nil, err)

	valI, exists := m.Get("o")
	s.Require().Equal(true, exists)
	s.Require().Equal(1, valI)

	err = m.Set("o", 2, 1)
	s.Require().Equal(nil, err)

	valI, exists = m.Get("o")
	s.Require().Equal(true, exists)
	s.Require().Equal(2, valI)
}

func (s *TTLMapSuite) TestRemoveExpiredEdgeCase() {
	clock := clockwork.NewFakeClock()
	m := newTTLMap(1, clock)

	err := m.Set("a", 1, 1)
	s.Require().Equal(nil, err)

	clock.Advance(1 * time.Second)

	err = m.Set("b", 2, 1)
	s.Require().Equal(nil, err)

	valI, exists := m.Get("a")
	s.Require().Equal(false, exists)

	valI, exists = m.Get("b")
	s.Require().Equal(true, exists)
	s.Require().Equal(2, valI)

	s.Require().Equal(1, m.Len())
}

func (s *TTLMapSuite) TestRemoveOutOfCapacity() {
	clock := clockwork.NewFakeClock()
	m := newTTLMap(2, clock)

	err := m.Set("a", 1, 5)
	s.Require().Equal(nil, err)

	clock.Advance(1 * time.Second)

	err = m.Set("b", 2, 6)
	s.Require().Equal(nil, err)

	err = m.Set("c", 3, 10)
	s.Require().Equal(nil, err)

	valI, exists := m.Get("a")
	s.Require().Equal(false, exists)

	valI, exists = m.Get("b")
	s.Require().Equal(true, exists)
	s.Require().Equal(2, valI)

	valI, exists = m.Get("c")
	s.Require().Equal(true, exists)
	s.Require().Equal(3, valI)

	s.Require().Equal(2, m.Len())
}

func (s *TTLMapSuite) TestGetNotExists() {
	m := NewTTLMap(1)
	_, exists := m.Get("a")
	s.Require().Equal(false, exists)
}

func (s *TTLMapSuite) TestGetIntNotExists() {
	m := NewTTLMap(1)
	_, exists, err := m.GetInt("a")
	s.Require().Equal(nil, err)
	s.Require().Equal(false, exists)
}

func (s *TTLMapSuite) TestGetInvalidType() {
	m := NewTTLMap(1)
	m.Set("a", "banana", 5)

	_, _, err := m.GetInt("a")
	s.Require().EqualError(err, "Expected existing value to be integer, got string")

	_, err = m.Increment("a", 4, 1)
	s.Require().EqualError(err, "Expected existing value to be integer, got string")
}

func (s *TTLMapSuite) TestIncrementGetExpire() {
	clock := clockwork.NewFakeClock()
	m := newTTLMap(1, clock)

	m.Increment("a", 5, 1)
	val, exists, err := m.GetInt("a")

	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(5, val)

	clock.Advance(1 * time.Second)

	m.Increment("a", 4, 1)
	val, exists, err = m.GetInt("a")

	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(4, val)
}

func (s *TTLMapSuite) TestIncrementOverwrite() {
	m := NewTTLMap(1)

	m.Increment("a", 5, 1)
	val, exists, err := m.GetInt("a")

	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(5, val)

	m.Increment("a", 4, 1)
	val, exists, err = m.GetInt("a")

	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(9, val)
}

func (s *TTLMapSuite) TestIncrementOutOfCapacity() {
	m := NewTTLMap(1)

	m.Increment("a", 5, 1)
	val, exists, err := m.GetInt("a")

	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(5, val)

	m.Increment("b", 4, 1)
	val, exists, err = m.GetInt("b")

	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(4, val)

	val, exists, err = m.GetInt("a")

	s.Require().Equal(nil, err)
	s.Require().Equal(false, exists)
}

func (s *TTLMapSuite) TestIncrementRemovesExpired() {
	clock := clockwork.NewFakeClock()
	m := newTTLMap(2, clock)

	m.Increment("a", 1, 1)
	m.Increment("b", 2, 2)

	clock.Advance(1 * time.Second)
	m.Increment("c", 3, 3)

	val, exists, err := m.GetInt("a")

	s.Require().Equal(nil, err)
	s.Require().Equal(false, exists)

	val, exists, err = m.GetInt("b")
	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(2, val)

	val, exists, err = m.GetInt("c")
	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(3, val)
}

func (s *TTLMapSuite) TestIncrementRemovesLastUsed() {
	m := NewTTLMap(2)

	m.Increment("a", 1, 10)
	m.Increment("b", 2, 11)
	m.Increment("c", 3, 12)

	val, exists, err := m.GetInt("a")

	s.Require().Equal(nil, err)
	s.Require().Equal(false, exists)

	val, exists, err = m.GetInt("b")
	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)

	s.Require().Equal(2, val)

	val, exists, err = m.GetInt("c")
	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(3, val)
}

func (s *TTLMapSuite) TestIncrementUpdatesTtl() {
	clock := clockwork.NewFakeClock()
	m := newTTLMap(1, clock)

	m.Increment("a", 1, 1)
	m.Increment("a", 1, 10)

	clock.Advance(1 * time.Second)

	val, exists, err := m.GetInt("a")
	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(2, val)
}

func (s *TTLMapSuite) TestUpdate() {
	clock := clockwork.NewFakeClock()
	m := newTTLMap(1, clock)

	m.Increment("a", 1, 1)
	m.Increment("a", 1, 10)

	clock.Advance(1 * time.Second)

	val, exists, err := m.GetInt("a")
	s.Require().Equal(nil, err)
	s.Require().Equal(true, exists)
	s.Require().Equal(2, val)
}

func (s *TTLMapSuite) TestCallOnExpire() {
	var called bool
	var key string
	var val interface{}
	clock := clockwork.NewFakeClock()
	m := newTTLMap(1, clock)
	m.OnExpire = func(k string, el interface{}) {
		called = true
		key = k
		val = el
	}

	err := m.Set("a", 1, 1)
	s.Require().Equal(nil, err)

	valI, exists := m.Get("a")
	s.Require().Equal(true, exists)
	s.Require().Equal(1, valI)

	clock.Advance(1 * time.Second)

	_, exists = m.Get("a")
	s.Require().Equal(false, exists)
	s.Require().Equal(true, called)
	s.Require().Equal("a", key)
	s.Require().Equal(1, val)
}

func newTTLMap(ttlSeconds int, clock clockwork.FakeClock) *TTLMap {
	m := NewTTLMap(ttlSeconds)
	m.clock = clock
	return m
}
