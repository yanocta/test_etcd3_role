package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	dialTimeout    = 2 * time.Second
	requestTimeout = 10 * time.Second
	endPoints      = []string{"localhost:2379", "localhost:22379", "localhost:32379"}
)

/*
func GetSingleValueDemo(ctx context.Context, kv clientv3.KV) {
	fmt.Println("*** GetSingleValueDemo()")
	// Delete all keys
	kv.Delete(ctx, "key", clientv3.WithPrefix())

	// Insert a key value
	pr, err := kv.Put(ctx, "key", "444")
	if err != nil {
		log.Fatal(err)
	}

	rev := pr.Header.Revision

	fmt.Println("Revision:", rev)

	gr, err := kv.Get(ctx, "key")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)

	// Modify the value of an existing key (create new revision)
	kv.Put(ctx, "key", "555")

	gr, _ = kv.Get(ctx, "key")
	fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)

	// Get the value of the previous revision
	gr, _ = kv.Get(ctx, "key", clientv3.WithRev(rev))
	fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)
}

func GetMultipleValuesWithPaginationDemo(ctx context.Context, kv clientv3.KV) {
	fmt.Println("*** GetMultipleValuesWithPaginationDemo()")
	// Delete all keys
	kv.Delete(ctx, "key", clientv3.WithPrefix())

	// Insert 50 keys
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key_%02d", i)
		kv.Put(ctx, k, strconv.Itoa(i))
	}

	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(10),
	}

	gr, err := kv.Get(ctx, "key", opts...)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("--- First page ---")
	for _, item := range gr.Kvs {
		fmt.Println(string(item.Key), string(item.Value))
	}

	lastKey := string(gr.Kvs[len(gr.Kvs)-1].Key)

	fmt.Println("--- Second page ---")
	opts = append(opts, clientv3.WithFromKey())
	gr, _ = kv.Get(ctx, lastKey, opts...)

	// Skipping the first item, which the last item from from the previous Get
	for _, item := range gr.Kvs[1:] {
		fmt.Println(string(item.Key), string(item.Value))
	}
}

func WatchDemo(ctx context.Context, cli *clientv3.Client, kv clientv3.KV) {
	fmt.Println("*** WatchDemo()")
	// Delete all keys
	kv.Delete(ctx, "key", clientv3.WithPrefix())

	stopChan := make(chan interface{})
	go func() {
		watchChan := cli.Watch(ctx, "key", clientv3.WithPrefix())
		for {
			select {
			case result := <-watchChan:
				for _, ev := range result.Events {
					fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				}
			case <-stopChan:
				fmt.Println("Done watching.")
				return
			}
		}
	}()

	// Insert some keys
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key_%02d", i)
		kv.Put(ctx, k, strconv.Itoa(i))
	}

	// Make sure watcher go routine has time to recive PUT events
	time.Sleep(time.Second)

	stopChan <- 1

	// Insert some more keys (no one is watching)
	for i := 10; i < 20; i++ {
		k := fmt.Sprintf("key_%02d", i)
		kv.Put(ctx, k, strconv.Itoa(i))
	}
}

func LeaseDemo(ctx context.Context, cli *clientv3.Client, kv clientv3.KV) {
	fmt.Println("*** LeaseDemo()")
	// Delete all keys
	kv.Delete(ctx, "key", clientv3.WithPrefix())

	gr, _ := kv.Get(ctx, "key")
	if len(gr.Kvs) == 0 {
		fmt.Println("No 'key'")
	}

	lease, err := cli.Grant(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}

	// Insert key with a lease of 1 second TTL
	kv.Put(ctx, "key", "value", clientv3.WithLease(lease.ID))

	gr, _ = kv.Get(ctx, "key")
	if len(gr.Kvs) == 1 {
		fmt.Println("Found 'key'")
	}

	// Let the TTL expire
	time.Sleep(3 * time.Second)

	gr, _ = kv.Get(ctx, "key")
	if len(gr.Kvs) == 0 {
		fmt.Println("No more 'key'")
	}
}
*/

func main() {
	// Start
	ctx, _ := context.WithTimeout(context.Background(), requestTimeout)
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: dialTimeout,
		Endpoints:   endPoints,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nCreate Done!")
	defer cli.Close()

	kv := clientv3.NewKV(cli)

	// =======================================================
	// Create One
	pr, err := PutOne(ctx, kv, "fookey", "12")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("\nCreate One Done")
	fmt.Println("Revision Number: ", pr.Header.Revision)
	// =======================================================
	// Get One
	gr, err := GetOne(ctx, kv, "fookey")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("\nGet One Done")
	fmt.Println("Key: ", string(gr.Kvs[0].Key))
	fmt.Println("Value: ", string(gr.Kvs[0].Value))
	fmt.Println("Create Revision: ", gr.Kvs[0].CreateRevision)
	fmt.Println("Mod Revision: ", gr.Kvs[0].ModRevision)

	// =======================================================
	// Get with Prefix
	_, err = PutOne(ctx, kv, "fooNew", "13")
	if err != nil {
		fmt.Println(err)
	}

	gpref, err := GetWithPrefix(ctx, kv, "foo")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("\nGet Multiple Done")
	for _, item := range gpref.Kvs {
		fmt.Println(string(item.Key), string(item.Value))
	}

	// =======================================================
	// Test Revision
	fmt.Println("\nTest Revision")
	pr = TestFuncPut(ctx, kv)

	TestFuncGetRevision(ctx, kv, pr)
}

func PutOne(ctx context.Context, kv clientv3.KV, key string, value string) (*clientv3.PutResponse, error) {
	pr, err := kv.Put(ctx, key, value)
	if err != nil {
		return nil, err
	}

	return pr, nil
}

func GetOne(ctx context.Context, kv clientv3.KV, key string) (*clientv3.GetResponse, error) {
	gr, err := kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return gr, nil
}

func GetWithPrefix(ctx context.Context, kv clientv3.KV, key string) (*clientv3.GetResponse, error) {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}
	gr, err := kv.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	return gr, nil
}

func DeleteOne(ctx context.Context, kv clientv3.KV, key string) {
	kv.Delete(ctx, key)

	return
}

func DeleteMultiple(ctx context.Context, kv clientv3.KV, keyPrefix string) {
	kv.Delete(ctx, keyPrefix, clientv3.WithPrefix())

	return
}

func TestFuncPut(ctx context.Context, kv clientv3.KV) *clientv3.PutResponse {
	pr, err := kv.Put(ctx, "key1", "444")
	if err != nil {
		return nil
	}

	return pr
}

func TestFuncGetRevision(ctx context.Context, kv clientv3.KV, pr *clientv3.PutResponse) {
	revNum := pr.Header.Revision

	for i := revNum; i > 0; i-- {

		gr, err := kv.Get(ctx, "key1", clientv3.WithRev(int64(i)))
		if err != nil {
			fmt.Println(err)
			return
		}
		if len(gr.Kvs) == 0 {
			return
		}

		fmt.Println("*** TestFunc()\n***Revision Number: ", i)
		fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)
		fmt.Println("Create Rev: ", gr.Kvs[0].CreateRevision, "Mod Rev: ", gr.Kvs[0].ModRevision)
		fmt.Println("Key Version: ", gr.Kvs[0].Version, "\n")
	}
}
