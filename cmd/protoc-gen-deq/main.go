//go:generate go-bindata -pkg main -o out.go out.go.tpl
//go:generate protoc --gogofaster_out=. example/greeter/greeter.proto example/greeter/greeter2.proto
//go:generate protoc --plugin=protoc-gen-deq=./run.sh --deq_out=. example/greeter/greeter.proto example/greeter/greeter2.proto

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	path "path/filepath"
	"strings"
	"text/template"

	proto "github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	plugin "github.com/gogo/protobuf/protoc-gen-gogo/plugin"
)

var wellKnownTypes = []string{
	"google/protobuf/any.proto",
	"google/protobuf/api.proto",
	"google/protobuf/duration.proto",
	"google/protobuf/empty.proto",
	"google/protobuf/field_mask.proto",
	"google/protobuf/source_context.proto",
	"google/protobuf/timestamp.proto",
	"google/protobuf/type.proto",
	"google/protobuf/wrappers.proto",
}

type File struct {
	Name       string
	Source     string
	Package    string
	Services   []Service
	Types      []Type
	HasMethods bool
	Imports    map[string]string
}

type Type struct {
	GoName        string
	GoRef         string
	GoEventRef    string
	GoPkg         string
	GoEventPkg    string
	ProtoFullName string
}

func NewType(name Name, current Name) Type {
	var pkg, eventPkg string
	if name.GoPkg() != current.GoPkg() || name.PkgOverride != "" {
		pkg = name.GoPkg() + "."
	}
	if name.GoEventPkg() != current.GoEventPkg() {
		eventPkg = name.GoEventPkg() + "."
	}
	return Type{
		GoName:        name.GoName(),
		GoRef:         pkg + name.GoName(),
		GoEventRef:    eventPkg + name.GoName(),
		GoPkg:         pkg,
		GoEventPkg:    eventPkg,
		ProtoFullName: name.ProtoName(),
	}
}

func (t Type) String() string {
	return t.GoName
}

type Service struct {
	Name    string
	Methods []Method
	File    File
	Types   []Type
}

type Method struct {
	Name    string
	InType  Type
	OutType Type
	Service Service
}

var EventPackageSuffix = "__deq"

func main() {

	input := new(plugin.CodeGeneratorRequest)
	response := new(plugin.CodeGeneratorResponse)

	inbuf, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		response.Error = proto.String(fmt.Sprintf("read input: %v", err))
		reply(response)
	}

	err = proto.Unmarshal(inbuf, input)
	if err != nil {
		response.Error = proto.String(fmt.Sprintf("unmarshal input: %v", err))
		reply(response)
	}

	files, err := generate(input)
	if err != nil {
		response.Error = proto.String(err.Error())
	} else {
		response.File = files
	}

	reply(response)
}

func reply(response *plugin.CodeGeneratorResponse) {
	outBuf, err := proto.Marshal(response)
	if err != nil {
		panic("marshal response: " + err.Error())
	}
	os.Stdout.Write(outBuf)
	if response.Error != nil {
		log.Printf("%s", *response.Error)
		os.Exit(1)
	}
}

func containsString(target string, candidates []string) bool {
	for _, str := range candidates {
		if str == target {
			return true
		}
	}
	return false
}

func generate(input *plugin.CodeGeneratorRequest) ([]*plugin.CodeGeneratorResponse_File, error) {

	// Parse parameter
	params, err := ParseParams(input.GetParameter())
	if err != nil {
		return nil, fmt.Errorf("parse parameters: %v", err)
	}

	fileNames := make(map[string]*descriptor.FileDescriptorProto, len(input.GetProtoFile())+len(wellKnownTypes))
	filePackages := make(map[string]*descriptor.FileDescriptorProto, len(input.GetProtoFile())+len(wellKnownTypes))
	for _, protofile := range input.GetProtoFile() {
		fileNames[protofile.GetName()] = protofile
		filePackages[protofile.GetPackage()] = protofile
	}

	outfiles := make([]*plugin.CodeGeneratorResponse_File, 0, len(input.GetFileToGenerate()))

	for _, filepath := range input.GetFileToGenerate() {
		protofile := fileNames[filepath]
		if protofile == nil {
			return nil, fmt.Errorf("file %s in file_to_generate not found in proto_file", filepath)
		}

		dir := path.Dir(protofile.GetName())
		baseFileName := path.Base(protofile.GetName())
		baseName := strings.TrimSuffix(baseFileName, path.Ext(baseFileName))

		override := params.ImportOverrides[protofile.GetName()]
		eventOverride := params.DEQImportOverrides[protofile.GetName()]

		fName := Name{
			FileDescriptor:   protofile,
			PkgOverride:      override,
			EventPkgOverride: eventOverride != "",
		}

		file := File{
			Name:     path.Join(dir, baseName+".pb.deq.go"),
			Source:   protofile.GetName(),
			Package:  fName.GoPkg(),
			Services: make([]Service, len(protofile.Service)),
			Types:    make([]Type, len(protofile.MessageType)),
			Imports:  make(map[string]string),
		}

		// Add an import for this file if there's an override
		if override != "" {
			file.Imports[fName.GoPkg()] = override
		}

		// Add imports for dependencies that need a package to be imported
		for _, dep := range protofile.GetDependency() {
			otherfile := fileNames[dep]
			if otherfile == nil {
				return nil, fmt.Errorf("dependency %q not found in input files", dep)
			}

			pkgName := Name{FileDescriptor: otherfile}.GoPkg()

			eventOverride := params.DEQImportOverrides[otherfile.GetName()]
			if eventOverride != "" {
				file.Imports[pkgName+EventPackageSuffix] = eventOverride
			}

			override := params.ImportOverrides[otherfile.GetName()]
			if override != "" {
				file.Imports[pkgName] = override
				continue
			}
			// Don't import files in the same directory if there's no package override
			if path.Dir(otherfile.GetName()) == path.Dir(protofile.GetName()) {
				continue
			}

			file.Imports[pkgName] = path.Dir(otherfile.GetName())
		}

		for j, mType := range protofile.GetMessageType() {
			name := Name{
				FileDescriptor:   protofile,
				Descriptor:       mType,
				PkgOverride:      fName.PkgOverride,
				EventPkgOverride: fName.EventPkgOverride,
			}
			file.Types[j] = NewType(name, fName)
		}

		for j, svc := range protofile.GetService() {

			methods := make([]Method, len(svc.GetMethod()))
			typeSet := make(map[Type]struct{})

			for k, method := range svc.GetMethod() {
				file.HasMethods = true
				inFileDescriptor := filePackages[protoPkg(method.GetInputType())]
				inProtoName := protoName(method.GetInputType())
				inName := Name{
					FileDescriptor:   inFileDescriptor,
					Descriptor:       findDescriptor(inProtoName, inFileDescriptor),
					PkgOverride:      params.ImportOverrides[inFileDescriptor.GetName()],
					EventPkgOverride: params.DEQImportOverrides[inFileDescriptor.GetName()] != "",
				}
				outFileDescriptor := filePackages[protoPkg(method.GetOutputType())]
				outProtoName := protoName(method.GetOutputType())
				outName := Name{
					FileDescriptor:   outFileDescriptor,
					Descriptor:       findDescriptor(outProtoName, outFileDescriptor),
					PkgOverride:      params.ImportOverrides[outFileDescriptor.GetName()],
					EventPkgOverride: params.DEQImportOverrides[outFileDescriptor.GetName()] != "",
				}
				methods[k] = Method{
					Name:    method.GetName(),
					InType:  NewType(inName, fName),
					OutType: NewType(outName, fName),
					Service: file.Services[j],
				}

				typeSet[methods[k].InType] = struct{}{}
				typeSet[methods[k].OutType] = struct{}{}
			}

			// Create set of all types referenced by this service only
			types := make([]Type, 0, len(typeSet))
			for t := range typeSet {
				types = append(types, t)
			}

			file.Services[j] = Service{
				Name:    svc.GetName(),
				File:    file,
				Methods: methods,
				Types:   types,
			}
		}

		outfile := &plugin.CodeGeneratorResponse_File{
			Name: &file.Name,
		}

		data, err := Asset("out.go.tpl")
		if err != nil {
			return nil, fmt.Errorf("load template: %v", err)
		}

		tpl, err := template.New("main").Parse(string(data))
		if err != nil {
			return nil, fmt.Errorf("parse template: %v", err)
		}

		outbuffer := new(bytes.Buffer)
		err = tpl.Execute(outbuffer, file)
		if err != nil {
			return nil, fmt.Errorf("run template: %v", err)
		}
		outfile.Content = proto.String(outbuffer.String())

		outfiles = append(outfiles, outfile)
	}

	return outfiles, nil
}

// CamelCase returns the CamelCased name.
// If there is an interior underscore followed by a lower case letter,
// drop the underscore and convert the letter to upper case.
// There is a remote possibility of this rewrite causing a name collision,
// but it's so remote we're prepared to pretend it's nonexistent - since the
// C++ generator lowercases names, it's extremely unlikely to have two fields
// with different capitalizations.
// In short, _my_field_name_2 becomes XMyFieldName_2.
func CamelCase(s string) string {
	if s == "" {
		return ""
	}
	t := make([]byte, 0, 32)
	i := 0
	if s[0] == '_' {
		// Need a capital letter; drop the '_'.
		t = append(t, 'X')
		i++
	}
	// Invariant: if the next letter is lower case, it must be converted
	// to upper case.
	// That is, we process a word at a time, where words are marked by _ or
	// upper case letter. Digits are treated as words.
	for ; i < len(s); i++ {
		c := s[i]
		if c == '_' && i+1 < len(s) && isASCIILower(s[i+1]) {
			continue // Skip the underscore in s.
		}
		if isASCIIDigit(c) {
			t = append(t, c)
			continue
		}
		// Assume we have a letter now - if not, it's a bogus identifier.
		// The next word is a sequence of characters that must start upper case.
		if isASCIILower(c) {
			c ^= ' ' // Make it a capital letter.
		}
		t = append(t, c) // Guaranteed not lower case.
		// Accept lower case sequence that follows.
		for i+1 < len(s) && isASCIILower(s[i+1]) {
			i++
			t = append(t, s[i])
		}
	}
	return string(t)
}

// Is c an ASCII lower-case letter?
func isASCIILower(c byte) bool {
	return 'a' <= c && c <= 'z'
}

// Is c an ASCII digit?
func isASCIIDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func LastComponent(s, sep string) string {
	parsed := strings.Split(s, sep)
	return parsed[len(parsed)-1]
}

func findDescriptor(name string, protofile *descriptor.FileDescriptorProto) *descriptor.DescriptorProto {
	for _, d := range protofile.GetMessageType() {
		if d.GetName() == LastComponent(name, ".") {
			return d
		}
	}
	return nil
}

func protoPkg(name string) string {
	name = strings.TrimPrefix(name, ".")
	split := strings.LastIndex(name, ".")
	if split == -1 {
		return ""
	}
	return name[:split]
}

func protoName(name string) string {
	return strings.TrimPrefix(name, ".")
}

type Name struct {
	FileDescriptor   *descriptor.FileDescriptorProto
	Descriptor       *descriptor.DescriptorProto
	PkgOverride      string
	EventPkgOverride bool
}

func (n Name) RawName() string {
	return strings.Join([]string{n.FileDescriptor.GetPackage(), n.Descriptor.GetName()}, ".")
}

func (n Name) GoName() string {
	return CamelCase(LastComponent(n.RawName(), "."))
}

func (n Name) ProtoName() string {
	return protoName(n.RawName())
}

func (n Name) ProtoPkg() string {
	return protoPkg(n.RawName())
}

func (n Name) GoFullPkg() string {
	pkg := n.FileDescriptor.GetOptions().GetGoPackage()
	if pkg != "" {
		split := strings.SplitN(pkg, ";", 2)
		if len(split) > 0 {
			return split[0]
		}
	}

	return strings.ReplaceAll(n.FileDescriptor.GetPackage(), ".", "_")
}

func (n Name) GoPkg() string {
	pkg := strings.SplitN(n.FileDescriptor.GetOptions().GetGoPackage(), ";", 2)
	if len(pkg) == 2 {
		return pkg[1]
	}

	split := strings.Split(n.GoFullPkg(), "/")
	return split[len(split)-1]
}

func (n Name) GoEventPkg() string {
	if n.EventPkgOverride {
		return n.GoPkg() + EventPackageSuffix
	}
	return n.GoPkg()
}
