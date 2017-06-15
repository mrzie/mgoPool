# mgoPool

> Not yet stable

```go
//usuage
import (
    "github.com/mrize/mgopool"
    //	"gopkg.in/mgo.v2"
)

var pool *mgoPool.Pool

func main() {
    pool = mgoPool.CreatePool(db_url, 10, 1000)
	if err != nil {
		log.Fatal(err)
	}
}

func readSth() (err error) {
    //var session *mgo.Session 
    
    session, err := pool.Aquire()
    if err != nil {
        return
    }
    defer pool.Release(session)

}

```