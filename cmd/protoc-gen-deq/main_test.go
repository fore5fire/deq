package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	path "path/filepath"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	plugin_go "github.com/gogo/protobuf/protoc-gen-gogo/plugin"
)

func TestProtocGenDEQ(t *testing.T) {
	dir, err := ioutil.TempDir("testdata", "test_out_")
	if err != nil {
		t.Fatalf("create test directory: %v", err)
	}
	// defer os.RemoveAll(dir)

	greeterDir := path.Join(dir, "example", "greeter")
	err = os.MkdirAll(greeterDir, 0700)
	if err != nil {
		t.Fatalf("create greeter directory: %v", err)
	}

	files, err := generate(testRequest)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != 2 {
		t.Errorf("count generated files: expected 2, got %d", len(files))
	}

	// Copy protoc-gen-gogofaster output to directory
	err = CopyTo(greeterDir, "example/greeter/greeter.pb.go")
	if err != nil {
		t.Fatalf("copy test data: %v", err)
	}

	for _, file := range files {
		filename := path.Join(dir, file.GetName())
		err = ioutil.WriteFile(filename, []byte(file.GetContent()), 0644)
		if err != nil {
			t.Fatalf("write file %s: %v", filename, err)
		}
	}

	cmd := exec.Command("go", "build")
	cmd.Dir = greeterDir
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	err = cmd.Run()
	if err != nil {
		t.Logf("go build: %v: %s\n", err, errBuf.Bytes())
		for _, file := range files {
			t.Logf("%s\n", file.GetContent())
		}
		t.FailNow()
	}
}

// Generated from example/greeter/greeter.proto and example/greeter/greeter2.proto. See
// TestGenerateTestRequest for re-generating this struct.
var testRequest = &plugin_go.CodeGeneratorRequest{
	FileToGenerate: []string{"example/greeter/greeter.proto", "example/greeter/greeter2.proto"},
	Parameter:      func(v string) *string { return &v }(""),
	ProtoFile: []*descriptor.FileDescriptorProto{
		&descriptor.FileDescriptorProto{
			Name:    func(v string) *string { return &v }("example/greeter/greeter.proto"),
			Package: func(v string) *string { return &v }("greeter"),
			MessageType: []*descriptor.DescriptorProto{
				{
					Name: func(v string) *string { return &v }("HelloRequest"),
					Field: []*descriptor.FieldDescriptorProto{{
						Name:     func(v string) *string { return &v }("name"),
						Number:   func(v int32) *int32 { return &v }(1),
						Label:    func(v descriptor.FieldDescriptorProto_Label) *descriptor.FieldDescriptorProto_Label { return &v }(1),
						Type:     func(v descriptor.FieldDescriptorProto_Type) *descriptor.FieldDescriptorProto_Type { return &v }(9),
						JsonName: func(v string) *string { return &v }("name"),
					}},
				},
				{
					Name: func(v string) *string { return &v }("HelloReply"),
					Field: []*descriptor.FieldDescriptorProto{{
						Name:     func(v string) *string { return &v }("message"),
						Number:   func(v int32) *int32 { return &v }(1),
						Label:    func(v descriptor.FieldDescriptorProto_Label) *descriptor.FieldDescriptorProto_Label { return &v }(1),
						Type:     func(v descriptor.FieldDescriptorProto_Type) *descriptor.FieldDescriptorProto_Type { return &v }(9),
						JsonName: func(v string) *string { return &v }("message"),
					}},
				},
			},
			EnumType: []*descriptor.EnumDescriptorProto{{
				Name: func(v string) *string { return &v }("HelloType"),
				Value: []*descriptor.EnumValueDescriptorProto{
					{
						Name:   func(v string) *string { return &v }("DEFAULT"),
						Number: func(v int32) *int32 { return &v }(0),
					},
					{
						Name:   func(v string) *string { return &v }("FRIENDLY"),
						Number: func(v int32) *int32 { return &v }(1),
					},
				},
			}},
			Service: []*descriptor.ServiceDescriptorProto{{
				Name: func(v string) *string { return &v }("Greeter"),
				Method: []*descriptor.MethodDescriptorProto{{
					Name:       func(v string) *string { return &v }("SayHello"),
					InputType:  func(v string) *string { return &v }(".greeter.HelloRequest"),
					OutputType: func(v string) *string { return &v }(".greeter.HelloReply"),
					Options: &descriptor.MethodOptions{
						XXX_InternalExtensions: proto.NewUnsafeXXX_InternalExtensions(map[int32]proto.Extension{}),
					},
				}},
			}},
			Syntax: func(v string) *string { return &v }("proto3"),
		},
		&descriptor.FileDescriptorProto{
			Name:    func(v string) *string { return &v }("google/protobuf/empty.proto"),
			Package: func(v string) *string { return &v }("google.protobuf"),
			MessageType: []*descriptor.DescriptorProto{{
				Name: func(v string) *string { return &v }("Empty")},
			},
			Options: &descriptor.FileOptions{
				JavaPackage:            func(v string) *string { return &v }("com.google.protobuf"),
				JavaOuterClassname:     func(v string) *string { return &v }("EmptyProto"),
				JavaMultipleFiles:      func(v bool) *bool { return &v }(true),
				GoPackage:              func(v string) *string { return &v }("github.com/golang/protobuf/ptypes/empty"),
				CcEnableArenas:         func(v bool) *bool { return &v }(true),
				ObjcClassPrefix:        func(v string) *string { return &v }("GPB"),
				CsharpNamespace:        func(v string) *string { return &v }("Google.Protobuf.WellKnownTypes"),
				XXX_InternalExtensions: proto.NewUnsafeXXX_InternalExtensions(map[int32]proto.Extension{}),
			},
			Syntax: func(v string) *string { return &v }("proto3"),
		},
		&descriptor.FileDescriptorProto{
			Name:       func(v string) *string { return &v }("example/greeter/greeter2.proto"),
			Package:    func(v string) *string { return &v }("greeter2"),
			Dependency: []string{"example/greeter/greeter.proto", "google/protobuf/empty.proto"},
			Service: []*descriptor.ServiceDescriptorProto{{
				Name: func(v string) *string { return &v }("Greeter2"),
				Method: []*descriptor.MethodDescriptorProto{
					{
						Name:       func(v string) *string { return &v }("SayHello"),
						InputType:  func(v string) *string { return &v }(".greeter.HelloRequest"),
						OutputType: func(v string) *string { return &v }(".greeter.HelloReply"),
						Options: &descriptor.MethodOptions{
							XXX_InternalExtensions: proto.NewUnsafeXXX_InternalExtensions(map[int32]proto.Extension{}),
						},
					},
					{
						Name:       func(v string) *string { return &v }("SayNothing"),
						InputType:  func(v string) *string { return &v }(".greeter.HelloRequest"),
						OutputType: func(v string) *string { return &v }(".google.protobuf.Empty"),
						Options: &descriptor.MethodOptions{
							XXX_InternalExtensions: proto.NewUnsafeXXX_InternalExtensions(map[int32]proto.Extension{}),
						},
					}},
			}},
			Options: &descriptor.FileOptions{
				GoPackage:              func(v string) *string { return &v }("greeter"),
				XXX_InternalExtensions: proto.NewUnsafeXXX_InternalExtensions(map[int32]proto.Extension{}),
			},
			Syntax: func(v string) *string { return &v }("proto3"),
		},
	},
}

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

func TestGenerateTestRequest(t *testing.T) {
	// Uncomment the next line to run the generator:
	t.Skip()

	descriptorPath := path.Join(os.TempDir(), "greeter_descriptor.pb")

	cmd := exec.Command("protoc", "--descriptor_set_out="+descriptorPath, "example/greeter/greeter.proto", "example/greeter/greeter2.proto", "google/protobuf/empty.proto")
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(descriptorPath)

	descriptors := new(descriptor.FileDescriptorSet)
	buf, err := ioutil.ReadFile(descriptorPath)
	if err != nil {
		t.Fatal(err)
	}

	err = proto.Unmarshal(buf, descriptors)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf(`
var testRequest = &plugin_go.CodeGeneratorRequest{
	FileToGenerate: []string{"greeter.proto", "greeter2.proto"},
	Parameter:      func(v string) *string { return &v }(""),
	ProtoFile:      %#v,
}
	`, descriptors.File)
	t.Error()
}
