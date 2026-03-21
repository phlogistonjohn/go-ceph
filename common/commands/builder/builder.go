package builder

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type CephTypeFunc func(*SignatureVar) CephArgumentType

type Builder struct {
	Values      map[string]any
	Description Description
	GetType     CephTypeFunc
}

func NewBuilder(d Description) *Builder {
	b := Builder{
		Values:      map[string]any{},
		Description: d,
		GetType:     BindArgumentType,
	}
	return b.Prepare()
}

func (b *Builder) Prepare() *Builder {
	b.Values["prefix"] = b.Description.PrefixString()
	return b
}

func (b *Builder) Arguments() []CephArgumentType {
	out := []CephArgumentType{}
	for _, v := range b.Description.Variables() {
		out = append(out, b.GetType(v))
	}
	return out
}

func (b *Builder) Validate() error {
	for _, t := range b.Arguments() {
		t.Validate(b.Values)
	}
	return nil
}

func (b *Builder) applyArgs(args []string) error {
	return nil
}

func (b *Builder) applyNamedArgs(args map[string]string) error {
	return nil
}

func (b *Builder) Apply(args []string, named map[string]string) error {
	if len(args) > 0 {
		if err := b.applyArgs(args); err != nil {
			return err
		}
	}
	if len(named) > 0 {
		if err := b.applyNamedArgs(named); err != nil {
			return err
		}
	}
	return nil
}

type CephArgumentType interface {
	TypeName() string
	Name() string
	Set(map[string]any, any) error
	Validate(map[string]any) error
}

type CephScalarArgumentType interface {
	CephArgumentType
	Convert(v any) (any, error)
	Check(v any) error
}

type CephMultiArgumentType interface {
	Append(map[string]any, any) error
}

/* Type: Ceph Choices */

type CephChoices struct {
	sv *SignatureVar
}

func (*CephChoices) TypeName() string { return "CephChoices" }

func (t *CephChoices) Name() string { return t.sv.Name }

func (t *CephChoices) Choices() map[string]bool {
	m := map[string]bool{}
	for _, ch := range strings.Split(t.sv.Choices, "|") {
		m[ch] = true
	}
	return m
}

func (t *CephChoices) choose(s string) (string, error) {
	if !t.Choices()[s] {
		return "", fmt.Errorf("invalid choice: %s", s)
	}
	return s, nil
}

func (t *CephChoices) Convert(v any) (any, error) {
	switch vs := v.(type) {
	case string:
		return t.choose(vs)
	case fmt.Stringer:
		return t.choose(vs.String())
	}
	return "", fmt.Errorf("not a string: %v", v)
}

func (t *CephChoices) Check(v any) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("not a string: %v (at %s)", v, t.sv.Name)
	}
	if !t.Choices()[s] {
		return fmt.Errorf("invalid choice: %s (at %s)", s, t.sv.Name)
	}
	return nil
}

func (t *CephChoices) Set(data map[string]any, v any) error {
	x, e := t.Convert(v)
	return save(t.sv, data, x, e)
}

func (t *CephChoices) Validate(data map[string]any) error {
	return checkEntry(t.sv, t, data)
}

/* Type: Ceph String */

type CephString struct {
	sv *SignatureVar
}

func (*CephString) TypeName() string { return "CephString" }

func (t *CephString) Name() string { return t.sv.Name }

func (t *CephString) Convert(v any) (any, error) {
	if s, ok := v.(string); ok {
		return s, nil
	}
	if ss, ok := v.(fmt.Stringer); ok {
		return ss.String(), nil
	}
	return "", fmt.Errorf("not a string: %v", v)
}

func (t *CephString) Check(v any) error {
	if _, ok := v.(string); !ok {
		return fmt.Errorf("not a string: %v (at %s)", v, t.sv.Name)
	}
	return nil
}

func (t *CephString) Set(data map[string]any, v any) error {
	x, e := t.Convert(v)
	return save(t.sv, data, x, e)
}

func (t *CephString) Validate(data map[string]any) error {
	return checkEntry(t.sv, t, data)
}

/* Type: Ceph Int */

type CephInt struct {
	sv *SignatureVar
}

func (*CephInt) TypeName() string { return "CephInt" }

func (t *CephInt) Name() string { return t.sv.Name }

func (t *CephInt) Convert(v any) (any, error) {
	switch vv := v.(type) {
	case int, int64, uint64, int32, uint32, int16, uint16, int8, uint8:
		return vv, nil
	case string:
		return strconv.ParseInt(vv, 10, 64)
	}
	return "", fmt.Errorf("not a CephInt: %v", v)
}

func (t *CephInt) Check(v any) error {
	switch v.(type) {
	case int, int64, uint64, int32, uint32, int16, uint16, int8, uint8:
		return nil
	}
	return fmt.Errorf("not a CephInt: %v (at %s)", v, t.sv.Name)
}

func (t *CephInt) Set(data map[string]any, v any) error {
	x, e := t.Convert(v)
	return save(t.sv, data, x, e)
}

func (t *CephInt) Validate(data map[string]any) error {
	return checkEntry(t.sv, t, data)
}

/* Type: Ceph Float */

type CephFloat struct {
	sv *SignatureVar
}

func (*CephFloat) TypeName() string { return "CephFloat" }

func (t *CephFloat) Name() string { return t.sv.Name }

func (t *CephFloat) Convert(v any) (any, error) {
	switch vv := v.(type) {
	case float64, float32:
		return vv, nil
	case string:
		return strconv.ParseFloat(vv, 64)
	}
	return "", fmt.Errorf("not a CephFloat: %v", v)
}

func (t *CephFloat) Check(v any) error {
	switch v.(type) {
	case float64, float32:
		return nil
	}
	return fmt.Errorf("not a float: %v (at %s)", v, t.sv.Name)
}

func (t *CephFloat) Set(data map[string]any, v any) error {
	x, e := t.Convert(v)
	return save(t.sv, data, x, e)
}

func (t *CephFloat) Validate(data map[string]any) error {
	return checkEntry(t.sv, t, data)
}

/* Type: Ceph Bool */

type CephBool struct {
	sv *SignatureVar
}

func (*CephBool) TypeName() string { return "CephBool" }

func (t *CephBool) Name() string { return t.sv.Name }

func (t *CephBool) Convert(v any) (any, error) {
	switch vv := v.(type) {
	case bool:
		return vv, nil
	case string:
		return strconv.ParseBool(vv)
	}
	return "", fmt.Errorf("not a CephBool: %v", v)
}

func (t *CephBool) Check(v any) error {
	if _, ok := v.(bool); !ok {
		return fmt.Errorf("not a bool: %v (at %s)", v, t.sv.Name)
	}
	return nil
}

func (t *CephBool) Set(data map[string]any, v any) error {
	x, e := t.Convert(v)
	return save(t.sv, data, x, e)
}

func (t *CephBool) Validate(data map[string]any) error {
	return checkEntry(t.sv, t, data)
}

/* Type: Ceph Pool Name */

type CephPoolName struct {
	CephString
}

func (*CephPoolName) TypeName() string { return "CephPoolname" }

/* Type: Ceph Object Name */

type CephObjectName struct {
	CephString
}

func (*CephObjectName) TypeName() string { return "CephObjectname" }

/* Type: Ceph OSD Name */

type CephOSDName struct {
	CephString
}

func (*CephOSDName) TypeName() string { return "CephOsdName" }

/* Type: Ceph PG ID */

type CephPGID struct {
	CephString
}

func (*CephPGID) TypeName() string { return "CephPgid" }

/* Type: Unknown */

type CephUnknownType struct {
	sv *SignatureVar
}

func (*CephUnknownType) TypeName() string { return "(Unknown)" }

func (t *CephUnknownType) Name() string { return t.sv.Name }

func (t *CephUnknownType) Set(data map[string]any, v any) error {
	return fmt.Errorf("Can not Set argument: Unknown Type: %s", t.sv.Type)
}

func (*CephUnknownType) Validate(data map[string]any) error {
	return nil
}

type CephRepeatedArg struct {
	inner CephScalarArgumentType
	sv    *SignatureVar
}

func (t *CephRepeatedArg) TypeName() string {
	return fmt.Sprintf("%s (Repeat: %s)", t.inner.TypeName(), t.sv.Repeat)
}

func (t *CephRepeatedArg) Name() string { return t.sv.Name }

func (t *CephRepeatedArg) Set(data map[string]any, v any) error {
	rval := reflect.ValueOf(v)
	if rval.Kind() == reflect.Slice {
		// is a slice
		data[t.sv.Name] = []any{} // reset field
		for i := 0; i < rval.Len(); i++ {
			if err := t.Append(data, rval.Index(i)); err != nil {
				return err
			}
		}
		return nil
	}
	return t.Append(data, v)
}

func (t *CephRepeatedArg) Append(data map[string]any, v any) error {
	key := t.sv.Name
	var temp []any
	if _, ok := data[key]; !ok {
		temp = []any{}
	} else {
		temp = data[key].([]any)
	}
	vv, err := t.inner.Convert(v)
	if err != nil {
		return err
	}
	data[key] = append(temp, vv)
	return nil
}

func (t *CephRepeatedArg) Validate(data map[string]any) error {
	if v, ok := data[t.sv.Name]; ok {
		if vv, ok := v.([]any); ok {
			for i := range vv {
				if err := t.inner.Check(vv[i]); err != nil {
					return nil
				}
			}
		} else {
			return fmt.Errorf("not a slice: %v (at %s)", v, t.sv.Name)
		}
	}
	if t.sv.Required() {
		return fmt.Errorf("missing required arg: %s", t.sv.Name)
	}
	return nil
}

func BindArgumentType(sv *SignatureVar) CephArgumentType {
	switch sv.Repeat {
	case "N":
		inner := getScalarArgumentType(sv)
		if st, ok := inner.(CephScalarArgumentType); ok {
			return &CephRepeatedArg{st, sv}
		}
		panic("inner type not a scalar type: " + inner.TypeName())
	}
	return getScalarArgumentType(sv)
}

func getScalarArgumentType(sv *SignatureVar) CephArgumentType {
	switch sv.Type {
	case "CephString":
		return &CephString{sv}
	case "CephChoices":
		return &CephChoices{sv}
	case "CephInt":
		return &CephInt{sv}
	case "CephFloat":
		return &CephFloat{sv}
	case "CephBool":
		return &CephBool{sv}
	case "CephPoolname":
		return &CephPoolName{CephString{sv}}
	case "CephObjectname":
		return &CephObjectName{CephString{sv}}
	case "CephOsdName":
		return &CephOSDName{CephString{sv}}
	case "CephPgid":
		return &CephPGID{CephString{sv}}
	}
	fmt.Printf("XXX: %v\n", sv.Type)
	return &CephUnknownType{sv}
}

func save(
	sv *SignatureVar, data map[string]any, v any, e error) error {
	// ---
	if e != nil {
		return e
	}
	data[sv.Name] = v
	return nil
}

func checkEntry(
	sv *SignatureVar, t CephScalarArgumentType, data map[string]any) error {
	// ---
	v, ok := data[sv.Name]
	if !ok {
		if sv.Required() {
			return fmt.Errorf("missing required arg: %s", sv.Name)
		}
		return nil
	}
	return t.Check(v)
}
