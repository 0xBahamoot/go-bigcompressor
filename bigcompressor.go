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

	"github.com/klauspost/compress/s2"
)

var chunkseparator = bytes.NewBufferString("_cHuNK_")

type BigCompressor struct {
	MaxPrecompressChunkSize int64
	MaxDecompressBufferSize int64
	CombineChunk            bool
	buffer                  *bytes.Buffer

	compressFile *os.File
}

type dataChunk struct {
	chunkNumber int
	files       []fileInfo
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
	for _, dataChunk := range dataChunks {
		err := bc.compressChunk(src, &dataChunk)
		if err != nil {
			return err
		}
		if !bc.CombineChunk {
			err = bc.writeChunk(dst + "_" + strconv.Itoa(dataChunk.chunkNumber))
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
		if len(n) > 100 {
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

func (bc *BigCompressor) decompressChunk(dst string) error {
	zr := s2.NewReader(bc.buffer)
	tr := tar.NewReader(zr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return err
		}
		target := filepath.Join(dst, header.Name)

		// check the type
		switch header.Typeflag {
		// if it's a file create it
		case tar.TypeReg:
			dirName := filepath.Dir(target)
			if _, serr := os.Stat(dirName); serr != nil {
				merr := os.MkdirAll(dirName, 0700)
				if merr != nil {
					panic(merr)
				}
			}
			fileToWrite, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			// copy over contents
			if _, err := io.Copy(fileToWrite, tr); err != nil {
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
	_, err := bc.compressFile.Write(append(bc.buffer.Bytes(), chunkseparator.Bytes()...))
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

func (bc *BigCompressor) compressChunk(src string, chunk *dataChunk) error {
	if bc.buffer == nil {
		bc.buffer = &bytes.Buffer{}
	}

	zr := s2.NewWriter(bc.buffer)
	tw := tar.NewWriter(zr)
	for _, fi := range chunk.files {
		header, err := tar.FileInfoHeader(fi, fi.file)

		if err != nil {
			return err
		}
		header.Name = strings.Replace(fi.file, src, "", 1)

		// write header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		// if not a dir, write file content
		if !fi.IsDir() {
			data, err := os.Open(fi.file)
			defer data.Close()
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, data); err != nil {
				return err
			}
		}
	}

	// produce tar
	if err := tw.Close(); err != nil {
		return err
	}
	// produce gzip
	if err := zr.Close(); err != nil {
		return err
	}
	return nil
}

func (bc BigCompressor) createChunkInfo(src string) []dataChunk {
	dataChunks := []dataChunk{}
	currentChunk := 0

	filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {
		if len(dataChunks)-1 != currentChunk {
			dataChunks = append(dataChunks, dataChunk{
				chunkNumber: currentChunk,
				files:       []fileInfo{},
				totalSize:   0,
			})
		}
		chunk := &dataChunks[currentChunk]
		if chunk.totalSize+fi.Size() <= bc.MaxPrecompressChunkSize {
			chunk.files = append(chunk.files, fileInfo{
				file:     file,
				FileInfo: fi,
			})
			if !fi.IsDir() {
				chunk.totalSize += fi.Size()
			}
		} else {
			currentChunk++
		}
		return nil
	})
	return dataChunks
}
