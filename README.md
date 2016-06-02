# AQUEUE - Append Only Queue

This code was built with the purpose of creating a persistent queue system without any kind of data update and that could be manipulated by commandline tools like cat, grep, sed and vi.

We never update or delete any byte.

* When we call a Push() one record is append to the end of push file
* When we call a Pop() one record is append to the end of pop file

There are two types of data files, push and pop.

###push

This file is open in append mode only, having the following structure:

*Length (int32) + space + CRC of payload (int32) + space + Payload (n bytes) + \n*

0000000011 2593144896 7 abcdefgh
0000000011 0157856902 8 abcdefgh
0000000011 3370543942 9 abcdefgh
0000000012 0745887787 10 abcdefgh
0000000012 3992684523 11 abcdefgh
0000000012 1947853290 12 abcdefgh

###pop

*CRC Pop (int32) + space + Payload offset + space + CRC Push record + \n*

0692834103 00000000000000000033 3014350275
4260429781 00000000000000000066 1915062787
2979655563 00000000000000000099 3955747842
2415368272 00000000000000000132 0709440450


##Example:

`
package main

import (
	"fmt"
	"github.com/ligadous/aqueue"
	"io"
	"log"
	"time"
)

func main() {

	//Start queue and create "data" forlder
	q, err := aqueue.New("data")
	if err != nil {
		log.Fatalf("ERRO) %s", err)
	}

	//Set max size to 100 MB, after that files will be rotate
	q.MaxSize(1e8)

	t1 := time.Now()

	//Writing 150 records into data/push file
	for i := 0; i < 150; i++ {
		q.Push([]byte(fmt.Sprintf("%d abcdefgh", i)))
	}

	//PopReadOnly - this will not change pop pointer position
	fmt.Printf("Reading 20 records\n")
	rec, _ := q.PopReadOnly(20)
	for _, v := range rec {
		fmt.Printf("%s\n", v)
	}

	fmt.Printf("\nReading again...\n")

	//Pop will change pop  to 100 position
	fmt.Printf("POP 160\n")
	for i := 0; i < 160; i++ {
		data, err := q.Pop()

		if err != nil && err != io.EOF {
			fmt.Printf("Erro: %s\n", err)
		}

		if len(data) > 0 {
			fmt.Printf("Rec: %s (len: %d)\n", data, len(data))
		} else {
			fmt.Printf("Queue empty\n")
		}
	}

	fmt.Printf("Time: %d\n", time.Since(t1))

	q.Close()
}
`

##TODO

* Stats information 
* LRU cache implementation
* commandline tools to manipulate queues files
	- remove lines
	- update lines
	- check CRC without pop and push