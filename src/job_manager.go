package main

import (
	"time"
	"log"
)

type JobManager struct {
}

func (jm *JobManager) start(id int) {
	log.Println("start job ", id)
	/* request for resource */

	/* bring up containers */

	/* monitor job execution */
	for {
		log.Println("executing job ", id)
		time.Sleep(time.Second * 5)
	}

	/* save logs etc. */

	/* return resource */
	log.Println("finish job", id)
}
