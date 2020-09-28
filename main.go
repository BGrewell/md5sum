package main

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
)

type result struct {
	path string
	sum  [md5.Size]byte
	err  error
}

func hasher(ctx context.Context, paths <-chan string, c chan<- result) {
	for path := range paths {
		data, err := ioutil.ReadFile(path)
		select {
		case c <- result{path, md5.Sum(data), err}:
		case <-ctx.Done():
			return
		}
	}
}

func walkRoot(ctx context.Context, root string) (<-chan string, <-chan error) {
	paths := make(chan string)
	errc := make(chan error, 1)

	go func() {
		defer close(paths)
		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !info.Mode().IsRegular() {
				return nil
			}

			select {
			case paths <- path:
			case <-ctx.Done():
				return errors.New("walk canceled")
			}
			return nil
		})
	}()

	return paths, errc
}

func handleCancel(ctx context.Context, cancel context.CancelFunc) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-c:
			fmt.Println("ctrl+c pressed. terminating")
			cancel()
			return
		case <-ctx.Done():
			return
		}
	}()
}

func HashAll(root string) (map[string][md5.Size]byte, error) {

	ctx, cancel := context.WithCancel(context.Background())
	handleCancel(ctx, cancel)

	paths, errc := walkRoot(ctx, root)

	c := make(chan result)
	var wg sync.WaitGroup

	const hasherCount = 20

	wg.Add(hasherCount)
	for i := 0; i < hasherCount; i++ {
		go func() {
			hasher(ctx, paths, c)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(c)
	}()

	m := make(map[string][md5.Size]byte)
	for r := range c {
		if r.err != nil {
			return nil, r.err
		}
		m[r.path] = r.sum
	}

	if err := <-errc; err != nil {
		return nil, err
	}

	return m, nil
}

func main() {

	if len(os.Args) <= 1 {
		fmt.Println("Error: you must provide the path to a file or directory")
		os.Exit(1)
	}

	m, err := HashAll(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	var paths []string
	for path := range m {
		paths = append(paths, path)
	}

	sort.Strings(paths)
	for _, path := range paths {
		fmt.Printf("%x %s\n", m[path], path)
	}
}
