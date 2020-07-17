package bigcompressor

import (
	"archive/tar"
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
)

var chunkseparator = bytes.NewBufferString("_cHuNK_")

type BigCompressor struct {
	MaxPrecompressChunkSize int64
	MaxDecompressBufferSize int64
	CombineChunk            bool
	buffer                  *bytes.Buffer

	ioCPBuffer   []byte
	compressFile *os.File
}

type dataChunk struct {
	chunkNumber int
	files       []*fileInfo
	totalSize   int64
}

type fileInfo struct {
	os.FileInfo
	file string
}

func (bc *BigCompressor) Compress(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		return err
	}
	dataChunks := bc.createChunkInfo(src)

	if bc.CombineChunk {
		var err error
		bc.compressFile, err = os.OpenFile(dst, os.O_CREATE|os.O_RDWR, 0700)
		defer bc.compressFile.Close()
		if err != nil {
			return err
		}
	}
	if bc.buffer == nil {
		bc.buffer = &bytes.Buffer{}
	}
	var dChunk *dataChunk
	if bc.ioCPBuffer == nil {
		bc.ioCPBuffer = make([]byte, 32*1024)
	}
	for _, dChunk = range dataChunks {
		err := bc.compressChunkNoAlloc(src, dChunk)
		if err != nil {
			return err
		}
		if !bc.CombineChunk {
			err = bc.writeChunk(dst + "_" + strconv.Itoa(dChunk.chunkNumber))
			if err != nil {
				return err
			}
		} else {
			err = bc.writeBufferToFile()
			if err != nil {
				return err
			}
		}
		bc.buffer.Reset()
	}
	return nil
}

func (bc *BigCompressor) Decompress(src, dst string) error {
	if bc.buffer == nil {
		bc.buffer = &bytes.Buffer{}
	}
	bc.buffer.Reset()
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	if bc.ioCPBuffer == nil {
		bc.ioCPBuffer = make([]byte, 32*1024)
	}
	csBytes := chunkseparator.Bytes()
	scanner := bufio.NewScanner(f)
	buf := make([]byte, bc.MaxDecompressBufferSize)
	scanner.Buffer(buf, bufio.MaxScanTokenSize)
	scanFn := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		commaidx := bytes.Index(data, csBytes)
		if commaidx > 0 {
			// we need to return the next position
			buffer := data[:commaidx]
			return commaidx + len(csBytes), bytes.TrimSpace(buffer), nil
		}
		// if we are at the end of the string, just return the entire buffer
		if atEOF {
			// but only do that when there is some data. If not, this might mean
			// that we've reached the end of our input CSV string
			if len(data) > 0 {
				return len(data), bytes.TrimSpace(data), nil
			}
		}

		// when 0, nil, nil is returned, this is a signal to the interface to read
		// more data in from the input reader. In this case, this input is our
		// string reader and this pretty much will never occur.
		return 0, nil, nil
	}
	scanner.Split(scanFn)
	for scanner.Scan() {
		n := scanner.Bytes()
		if len(n) > 10 {
			_, err = bc.buffer.Write(n)
			if err != nil {
				return err
			}
			err = bc.decompressChunk(dst)
			if err != nil {
				return err
			}
			bc.buffer.Reset()
		}

	}
	return nil
}

var zstdDecode *zstd.Decoder
var tarDecode *tar.Reader

func (bc BigCompressor) decompressChunk(dst string) error {
	if zstdDecode == nil {
		zstdDecode, _ = zstd.NewReader(bc.buffer)
		tarDecode = tar.NewReader(zstdDecode)
	} else {
		zstdDecode.Reset(bc.buffer)
	}

	var target, dirName string
	for {
		header, err := tarDecode.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return err
		}
		target = filepath.Join(dst, header.Name)

		// check the type
		switch header.Typeflag {
		// if it's a file create it
		case tar.TypeReg:
			dirName = filepath.Dir(target)
			if _, err = os.Stat(dirName); err != nil {
				err = os.MkdirAll(dirName, 0700)
				if err != nil {
					panic(err)
				}
			}
			fileToWrite, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			// copy over contents
			if _, err = io.CopyBuffer(fileToWrite, tarDecode, bc.ioCPBuffer); err != nil {
				return err
			}
			// manually close here after each file operation; defering would cause each file close
			// to wait until all operations have completed.
			fileToWrite.Close()
		}
	}
	return nil
}

func (bc *BigCompressor) writeBufferToFile() error {
	_, err := bc.compressFile.Write(bc.buffer.Bytes())
	if err != nil {
		return err
	}
	_, err = bc.compressFile.Write(chunkseparator.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (bc *BigCompressor) writeChunk(chunkDst string) error {
	fileToWrite, err := os.OpenFile(chunkDst, os.O_CREATE|os.O_RDWR, 0700)
	defer fileToWrite.Close()
	if err != nil {
		return err
	}
	if _, err := io.Copy(fileToWrite, bc.buffer); err != nil {
		return err
	}
	return nil
}

// func (bc *BigCompressor) compressChunk(src string, chunk *dataChunk) error {
// 	if bc.buffer == nil {
// 		bc.buffer = &bytes.Buffer{}
// 	}

// 	zr, _ := zstd.NewWriter(bc.buffer)
// 	tw := tar.NewWriter(zr)
// 	for _, fi := range chunk.files {
// 		header, err := tar.FileInfoHeader(fi, fi.file)

// 		if err != nil {
// 			return err
// 		}
// 		header.Name = strings.Replace(fi.file, src, "", 1)

// 		// write header
// 		if err := tw.WriteHeader(header); err != nil {
// 			return err
// 		}
// 		// if not a dir, write file content
// 		if !fi.IsDir() {
// 			data, err := os.Open(fi.file)
// 			defer data.Close()
// 			if err != nil {
// 				return err
// 			}
// 			if _, err := io.CopyBuffer(tw, data, bc.ioCPBuffer); err != nil {
// 				return err
// 			}
// 		}
// 	}

// 	// produce tar
// 	if err := tw.Close(); err != nil {
// 		return err
// 	}
// 	// produce gzip
// 	if err := zr.Close(); err != nil {
// 		return err
// 	}
// 	return nil
// }

func (bc BigCompressor) createChunkInfo(src string) []*dataChunk {
	dataChunks := []*dataChunk{}
	currentChunk := 0

	dataChunks = append(dataChunks, &dataChunk{
		chunkNumber: currentChunk,
		files:       []*fileInfo{},
		totalSize:   0,
	})

	filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {
		chunk := dataChunks[currentChunk]
		if chunk.totalSize+fi.Size() <= bc.MaxPrecompressChunkSize {
			chunk.files = append(chunk.files, &fileInfo{
				file:     file,
				FileInfo: fi,
			})
			if !fi.IsDir() {
				chunk.totalSize += fi.Size()
			}
		} else {
			currentChunk++
			dataChunks = append(dataChunks, &dataChunk{
				chunkNumber: currentChunk,
				files:       []*fileInfo{},
				totalSize:   0,
			})
			chunk = dataChunks[currentChunk]
			chunk.files = append(chunk.files, &fileInfo{
				file:     file,
				FileInfo: fi,
			})
			if !fi.IsDir() {
				chunk.totalSize += fi.Size()
			}
		}
		return nil
	})
	return dataChunks
}

var zstdEncode *zstd.Encoder
var tarEncode *tar.Writer

func (bc *BigCompressor) compressChunkNoAlloc(src string, chunk *dataChunk) error {
	if zstdEncode == nil {
		zstdEncode, _ = zstd.NewWriter(bc.buffer)
		tarEncode = tar.NewWriter(zstdEncode)
	} else {
		zstdEncode.Reset(bc.buffer)
	}
	var fi *fileInfo
	for _, fi = range chunk.files {
		header, err := tar.FileInfoHeader(fi, fi.file)

		if err != nil {
			return err
		}
		header.Name = strings.Replace(fi.file, src, "", 1)

		// write header
		if err := tarEncode.WriteHeader(header); err != nil {
			return err
		}
		// if not a dir, write file content
		if !fi.IsDir() {
			data, err := os.Open(fi.file)
			if err != nil {
				return err
			}
			if _, err := io.CopyBuffer(tarEncode, data, bc.ioCPBuffer); err != nil {
				return err
			}
			data.Close()
		}
	}

	// produce tar
	if err := tarEncode.Flush(); err != nil {
		return err
	}
	// produce gzip
	if err := zstdEncode.Close(); err != nil {
		return err
	}
	return nil
}
