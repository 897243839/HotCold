package hotclod

import (
	"fmt"
	com "github.com/897243839/Compress"
	"github.com/ipfs/go-datastore"
	"os"
	"path/filepath"
	"time"
)

var (
	block_hot                       = "blockhot.json"
	compressflag                    = "compressflag.json"
	maphot                          = New[int]()
	mapLit                          = New[int]()
	Mode         com.CompressorType = 1
)

// var maps sync.RWMutex
// var myTimer = time.Now().Unix() // 启动定时器
var ticker = time.NewTicker(60 * time.Second)     //计时器
var ticker1 = time.NewTicker(30000 * time.Minute) //计时器

var cb = func(exists bool, valueInMap int, newValue int) int {
	if !exists {
		return newValue
	}
	if valueInMap > 999 {
		return 1000
	}
	valueInMap += newValue
	return valueInMap

}
var ps = &Datastore{}

func putfs(fs *Datastore) {
	ps = fs
}

//	func init() {
//		go func() {
//			for {
//				select {
//				case <-ticker.C:
//					Pr()
//					updata_hc()
//				//default:
//				}
//			}
//		}()
//		go func() {
//			for  {
//				select {
//				case <-ticker1.C:
//					for key,v:=range maphot.Items(){
//						if v<=9{
//							dir := filepath.Join(ps.path, ps.getDir(key))
//							file := filepath.Join(dir, key+extension)
//							ps.Get_writer(dir,file)
//							maphot.Remove(key)
//							mapw:=maphot.Items()
//							ps.WriteBlockhotFile(mapw,true)
//						}else {
//							maphot.Set(key,1)
//						}
//					}
//					fmt.Println("更新本地热数据表成功")
//				}
//			}
//
//		}()
//	}

func Updatemaphot() {

	for key, v := range maphot.Items() {
		if v <= 9 {
			dir := filepath.Join(ps.path, ps.getDir(key))
			file := filepath.Join(dir, key+extension)
			ps.Get_writer(dir, file)
			maphot.Remove(key)
		} else {
			maphot.Set(key, 1)
		}
	}
	mapw := maphot.Items()
	ps.WriteJson(mapw, true, block_hot, ps.path)
	fmt.Println("本地热数据更新&&保存成功")
}

//func hc(key string) ([]byte, bool) {
//	data, f := hclist.Get(key)
//	return data, f
//}
//func put_hc(key string, data []byte) {
//	hclist.Set(key, data)
//}
//func updata_hc() {
//	println("缓冲大小", hclist.Count())
//	hclist.Clear()
//	println("缓冲大小", hclist.Count())
//}

func Pr() {
	mapLit.Clear()
}
func Jl(key string) {
	//------------------------------------------------------------
	mapLit.Upsert(key, 1, cb)
}
func Deljl(key string) {
	//---------------------------------------------------------------------
	mapLit.Remove(key)

}
func getmap(key string) int {
	n, _ := mapLit.Get(key)
	return n
}
func (fs *Datastore) dohotPut(key datastore.Key, val []byte) error {

	dir, path := fs.encode(key)
	if err := fs.makeDir(dir); err != nil {
		return err
	}

	tmp, err := fs.tempFile()
	if err != nil {
		return err
	}
	closed := false
	removed := false
	defer func() {
		if !closed {
			// silence errcheck
			_ = tmp.Close()
		}
		if !removed {
			// silence errcheck
			_ = os.Remove(tmp.Name())
		}
	}()

	if _, err := tmp.Write(val); err != nil {
		return err
	}
	if fs.sync {
		if err := syncFile(tmp); err != nil {
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	closed = true

	err = fs.renameAndUpdateDiskUsage(tmp.Name(), path)
	if err != nil {
		return err
	}
	removed = true

	if fs.sync {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	return nil
}

func (fs *Datastore) Get_writer(dir string, path string) (err error) {

	data, err := readFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return datastore.ErrNotFound
		}
		// no specific error to return, so just pass it through
		return err
	}

	fs.shutdownLock.RLock()
	defer fs.shutdownLock.RUnlock()
	if fs.shutdown {
		return ErrClosed
	}

	if err := fs.makeDir(dir); err != nil {
		return err
	}

	tmp, err := fs.tempFile()
	if err != nil {
		return err
	}

	//压缩

	//Jl(key.String())
	data = com.Compress(data, Mode)
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if fs.sync {
		if err := syncFile(tmp); err != nil {
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	err = fs.renameAndUpdateDiskUsage(tmp.Name(), path)
	if err != nil {
		return err
	}
	if fs.sync {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	defer tmp.Close()
	defer os.Remove(tmp.Name())
	fmt.Printf("get_writer触发\n")

	return nil
}

// readBlockhotFile is only safe to call in Open()
func (fs *Datastore) readJson(path string, name string) (map[string]int, int) {
	fpath := filepath.Join(path, name)
	duB, err := readFile(fpath)
	if err != nil {
		println("读json错误")
		return nil, 0
	}
	temp := make(map[string]int)
	err = json.Unmarshal(duB, &temp)
	if err != nil {
		println("读json错误")
		return nil, 0
	}

	return temp, 1
}
func (fs *Datastore) WriteJson(hot map[string]int, doSync bool, name string, path string) {
	tmp, err := fs.tempFile()
	if err != nil {
		log.Warnw("could not write hot usage", "error", err)
		return
	}

	removed := false
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		if !removed {
			// silence errcheck
			_ = os.Remove(tmp.Name())
		}

	}()

	encoder := json.NewEncoder(tmp)
	if err := encoder.Encode(hot); err != nil {
		log.Warnw("cound not write block hot", "error", err)
		return
	}
	if doSync {
		if err := tmp.Sync(); err != nil {
			log.Warnw("cound not sync", "error", err, "file", DiskUsageFile)
			return
		}
	}
	if err := tmp.Close(); err != nil {
		log.Warnw("cound not write block hot", "error", err)
		return
	}
	closed = true
	if err := rename(tmp.Name(), filepath.Join(path, name)); err != nil {
		log.Warnw("cound not write block hot", "error", err)
		return
	}
	removed = true
}
