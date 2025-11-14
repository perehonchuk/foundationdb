/*
 * queue.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const API_VERSION int = 740

type EmptyQueueError struct{}

func (q EmptyQueueError) Error() string {
	return "Queue is Empty"
}

func clear_subspace(trtr fdb.Transactor, sub subspace.Subspace) error {
	_, err := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(sub)
		return nil, nil
	})
	return err
}

type Queue struct {
	QueueSS subspace.Subspace
}

func (q *Queue) NewQueue(ss subspace.Subspace) {
	q.QueueSS = ss
}

func (q *Queue) Dequeue(trtr fdb.Transactor) (interface{}, error) {
	i, err := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		item, err := q.FirstItem(tr)
		if err != nil {
			return nil, err
		}
		tr.Clear(item.(fdb.KeyValue).Key)
		return item.(fdb.KeyValue).Value, err
	})
	return i, err
}

func (q *Queue) DequeueBatch(trtr fdb.Transactor, count int) ([]interface{}, error) {
	result, err := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		r, err := tr.GetRange(q.QueueSS, fdb.RangeOptions{Limit: count}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(r) == 0 {
			return []interface{}{}, nil
		}

		items := make([]interface{}, len(r))
		for i, kv := range r {
			items[i] = kv.Value
			tr.Clear(kv.Key)
		}

		return items, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]interface{}), nil
}

func (q *Queue) Enqueue(trtr fdb.Transactor, item interface{}) (interface{}, error) {
	i, err := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		index, err := q.LastIndex(tr)
		if err != nil {
			return nil, err
		}

		ki, err := q.QueueSS.Unpack(index.(fdb.Key))
		if err != nil {
			return nil, err
		}

		tr.Set(q.QueueSS.Pack(tuple.Tuple{ki[0].(int64) + 1}), []byte(item.(string)))

		return nil, nil
	})
	return i, err
}

func (q *Queue) EnqueueBatch(trtr fdb.Transactor, items []interface{}) (interface{}, error) {
	i, err := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		index, err := q.LastIndex(tr)
		if err != nil {
			return nil, err
		}

		ki, err := q.QueueSS.Unpack(index.(fdb.Key))
		if err != nil {
			return nil, err
		}

		startIndex := ki[0].(int64)
		for idx, item := range items {
			tr.Set(q.QueueSS.Pack(tuple.Tuple{startIndex + int64(idx) + 1}), []byte(item.(string)))
		}

		return len(items), nil
	})
	return i, err
}

func (q *Queue) LastIndex(trtr fdb.Transactor) (interface{}, error) {
	i, e := trtr.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		r, err := rtr.Snapshot().GetRange(q.QueueSS, fdb.RangeOptions{1, 0, true}).GetSliceWithError()
		if len(r) == 0 {
			return q.QueueSS.Pack(tuple.Tuple{0}), nil
		}
		return r[0].Key, err
	})
	return i, e
}

func (q *Queue) FirstItem(trtr fdb.Transactor) (interface{}, error) {
	i, e := trtr.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		r, err := rtr.GetRange(q.QueueSS, fdb.RangeOptions{1, 0, false}).GetSliceWithError()
		if len(r) == 0 {
			return nil, EmptyQueueError{}
		}
		return r[0], err
	})
	return i, e
}

func main() {
	fmt.Println("Queue Example Program")

	fdb.MustAPIVersion(API_VERSION)

	db := fdb.MustOpenDefault()

	QueueDemoDir, err := directory.CreateOrOpen(db, []string{"Queue"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	clear_subspace(db, QueueDemoDir)

	var q Queue
	q.NewQueue(QueueDemoDir.Sub("Queue"))

	// Test single enqueue/dequeue
	q.Enqueue(db, "test")
	q.Enqueue(db, "test1")

	// Test batch enqueue - efficiently add multiple items in one transaction
	batchItems := []interface{}{"batch1", "batch2", "batch3", "batch4", "batch5"}
	count, err := q.EnqueueBatch(db, batchItems)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Enqueued %v items in batch\n", count)

	// Test batch dequeue - efficiently retrieve multiple items in one transaction
	items, err := q.DequeueBatch(db, 3)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Dequeued batch:")
	for _, item := range items {
		fmt.Println("  -", string(item.([]byte)))
	}

	// Dequeue remaining items one by one
	for i := 0; i < 5; i++ {
		item, err := q.Dequeue(db)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(item.([]byte)))
	}
}
