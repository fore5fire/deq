package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	plugin_go "github.com/gogo/protobuf/protoc-gen-gogo/plugin"
)

func TestProtocGenDEQ(t *testing.T) {
	dir, err := ioutil.TempDir("testdata", "test_out_")
	if err != nil {
		t.Fatalf("create test directory: %v", err)
	}
	defer os.RemoveAll(dir)
	fmt.Println(dir)

	files, err := generate(testRequest)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != 1 {
		t.Errorf("count generated files: expected 1, got %d", len(files))
	}

	// Copy protoc-gen-gogofaster output to directory
	err = CopyTo(dir, "testdata/greeter.pb.go")
	if err != nil {
		t.Fatalf("copy test data: %v", err)
	}

	for _, file := range files {
		filename := path.Join(dir, file.GetName())
		err = ioutil.WriteFile(
			filename, []byte(file.GetContent()), 0644)
		if err != nil {
			t.Fatalf("write file %s: %v", filename, err)
		}
	}

	cmd := exec.Command("go", "build")
	cmd.Dir = dir
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	err = cmd.Run()
	if err != nil {
		t.Fatalf("go build: %v: %s\n%s", err, errBuf.Bytes(), files[0].GetContent())
	}
}

// Generated from tesdata/greeter.proto
var testRequest = &plugin_go.CodeGeneratorRequest{
	FileToGenerate: []string{"greeter.proto"}, Parameter: (*string)(nil), ProtoFile: []*descriptor.FileDescriptorProto{&descriptor.FileDescriptorProto{Name: func(v string) *string { return &v }("greeter.proto"),
		Package: func(v string) *string { return &v }("example.greeter"),
		MessageType: []*descriptor.DescriptorProto{&descriptor.DescriptorProto{Name: func(v string) *string { return &v }("HelloRequest"),
			Field: []*descriptor.FieldDescriptorProto{&descriptor.FieldDescriptorProto{Name: func(v string) *string { return &v }("name"),
				Number:   func(v int32) *int32 { return &v }(1),
				Label:    func(v descriptor.FieldDescriptorProto_Label) *descriptor.FieldDescriptorProto_Label { return &v }(1),
				Type:     func(v descriptor.FieldDescriptorProto_Type) *descriptor.FieldDescriptorProto_Type { return &v }(9),
				JsonName: func(v string) *string { return &v }("name"),
			}},
		}, &descriptor.DescriptorProto{Name: func(v string) *string { return &v }("HelloReply"),
			Field: []*descriptor.FieldDescriptorProto{&descriptor.FieldDescriptorProto{Name: func(v string) *string { return &v }("message"),
				Number:   func(v int32) *int32 { return &v }(1),
				Label:    func(v descriptor.FieldDescriptorProto_Label) *descriptor.FieldDescriptorProto_Label { return &v }(1),
				Type:     func(v descriptor.FieldDescriptorProto_Type) *descriptor.FieldDescriptorProto_Type { return &v }(9),
				JsonName: func(v string) *string { return &v }("message"),
			}},
		}},
		Service: []*descriptor.ServiceDescriptorProto{&descriptor.ServiceDescriptorProto{Name: func(v string) *string { return &v }("Greeter"),
			Method: []*descriptor.MethodDescriptorProto{&descriptor.MethodDescriptorProto{Name: func(v string) *string { return &v }("SayHello"),
				InputType:  func(v string) *string { return &v }(".example.greeter.HelloRequest"),
				OutputType: func(v string) *string { return &v }(".example.greeter.HelloReply"),
			}},
		}},
		SourceCodeInfo: &descriptor.SourceCodeInfo{
			Location: []*descriptor.SourceCodeInfo_Location{
				&descriptor.SourceCodeInfo_Location{
					Span: []int32{0, 0, 18, 1},
				},
				&descriptor.SourceCodeInfo_Location{
					Path: []int32{12},
					Span: []int32{0, 0, 18},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{2},
					Span: []int32{2, 0, 24},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{6, 0},
					Span:            []int32{5, 0, 8, 1},
					LeadingComments: func(v string) *string { return &v }(" The greeter service definition.\n"),
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{6, 0, 1},
					Span: []int32{5, 8, 15},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{6, 0, 2, 0},
					Span:            []int32{7, 2, 53},
					LeadingComments: func(v string) *string { return &v }(" Sends a greeting\n"),
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{6, 0, 2, 0, 1},
					Span: []int32{7, 6, 14},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{6, 0, 2, 0, 2},
					Span: []int32{7, 16, 28},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{6, 0, 2, 0, 3},
					Span: []int32{7, 39, 49},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 0},
					Span:            []int32{11, 0, 13, 1},
					LeadingComments: func(v string) *string { return &v }(" The request message containing the user's name.\n"),
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 0, 1},
					Span: []int32{11, 8, 20},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 0, 2, 0},
					Span: []int32{12, 2, 18},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 0, 2, 0, 4},
					Span: []int32{12, 2, 11, 22},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 0, 2, 0, 5},
					Span: []int32{12, 2, 8},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 0, 2, 0, 1},
					Span: []int32{12, 9, 13},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 0, 2, 0, 3},
					Span: []int32{12, 16, 17},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 1},
					Span:            []int32{16, 0, 18, 1},
					LeadingComments: func(v string) *string { return &v }(" The response message containing the greetings\n"),
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 1, 1},
					Span: []int32{16, 8, 18},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 1, 2, 0},
					Span: []int32{17, 2, 21},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 1, 2, 0, 4},
					Span: []int32{17, 2, 16, 20},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 1, 2, 0, 5},
					Span: []int32{17, 2, 8},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 1, 2, 0, 1},
					Span: []int32{17, 9, 16},
				},
				&descriptor.SourceCodeInfo_Location{Path: []int32{4, 1, 2, 0, 3},
					Span: []int32{17, 19, 20},
				}},
		},
		Syntax: func(v string) *string { return &v }("proto3"),
	}}, XXX_NoUnkeyedLiteral: struct{}{}, XXX_unrecognized: []uint8(nil), XXX_sizecache: 0}

func CopyTo(dstDir string, sources ...string) error {
	for _, src := range sources {
		outfile := path.Join(dstDir, path.Base(src))

		out, err := os.Open(src)
		if err != nil {
			return err
		}
		defer out.Close()

		in, err := os.Create(outfile)
		if err != nil {
			return err
		}
		defer in.Close()

		if _, err = io.Copy(in, out); err != nil {
			return err
		}
		err = out.Sync()
		if err != nil {
			return err
		}
	}

	return nil
}
