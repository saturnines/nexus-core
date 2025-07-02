// pkg/transform/transform.go
package transform

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Transformer defines the interface for field transformations
type Transformer interface {
	Transform(value interface{}) (interface{}, error)
}

// Registry holds all available transformers
type Registry struct {
	transformers map[string]TransformCreator
}

// TransformCreator creates a transformer from config
type TransformCreator func(config map[string]interface{}) (Transformer, error)

// NewRegistry creates a new transformer registry with defaults
func NewRegistry() *Registry {
	r := &Registry{
		transformers: make(map[string]TransformCreator),
	}

	// Register default transformers
	r.Register("string", stringTransformCreator)
	r.Register("int", intTransformCreator)
	r.Register("float", floatTransformCreator)
	r.Register("bool", boolTransformCreator)
	r.Register("date", dateTransformCreator)
	r.Register("split", splitTransformCreator)
	r.Register("join", joinTransformCreator)
	r.Register("upper", upperTransformCreator)
	r.Register("lower", lowerTransformCreator)
	r.Register("trim", trimTransformCreator)

	return r
}

// Register adds a new transformer type
func (r *Registry) Register(name string, creator TransformCreator) {
	r.transformers[name] = creator
}

// Create builds a transformer from config
func (r *Registry) Create(transformType string, config map[string]interface{}) (Transformer, error) {
	creator, ok := r.transformers[transformType]
	if !ok {
		return nil, fmt.Errorf("unknown transform type: %s", transformType)
	}
	return creator(config)
}

// StringTransform converts values to strings
type StringTransform struct{}

func stringTransformCreator(config map[string]interface{}) (Transformer, error) {
	return &StringTransform{}, nil
}

func (t *StringTransform) Transform(value interface{}) (interface{}, error) {
	if value == nil {
		return "", nil
	}
	return fmt.Sprintf("%v", value), nil
}

// IntTransform converts values to integers
type IntTransform struct{}

func intTransformCreator(config map[string]interface{}) (Transformer, error) {
	return &IntTransform{}, nil
}

func (t *IntTransform) Transform(value interface{}) (interface{}, error) {
	if value == nil {
		return 0, nil
	}

	switch v := value.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	case string:
		return strconv.Atoi(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to int", value)
	}
}

// FloatTransform converts values to floats
type FloatTransform struct{}

func floatTransformCreator(config map[string]interface{}) (Transformer, error) {
	return &FloatTransform{}, nil
}

func (t *FloatTransform) Transform(value interface{}) (interface{}, error) {
	if value == nil {
		return 0.0, nil
	}

	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0.0, fmt.Errorf("cannot convert %T to float", value)
	}
}

// BoolTransform converts values to booleans
type BoolTransform struct{}

func boolTransformCreator(config map[string]interface{}) (Transformer, error) {
	return &BoolTransform{}, nil
}

func (t *BoolTransform) Transform(value interface{}) (interface{}, error) {
	if value == nil {
		return false, nil
	}

	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	case int:
		return v != 0, nil
	case float64:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// DateTransform handles date/time transformations
type DateTransform struct {
	InputFormat  string
	OutputFormat string
}

func dateTransformCreator(config map[string]interface{}) (Transformer, error) {
	t := &DateTransform{
		InputFormat:  "RFC3339", // default
		OutputFormat: "RFC3339", // default
	}

	if inputFmt, ok := config["input_format"].(string); ok {
		t.InputFormat = inputFmt
	}
	if outputFmt, ok := config["output_format"].(string); ok {
		t.OutputFormat = outputFmt
	}

	return t, nil
}

func (t *DateTransform) Transform(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	var inputTime time.Time
	var err error

	switch v := value.(type) {
	case string:
		inputTime, err = t.parseTime(v, t.InputFormat)
		if err != nil {
			return nil, err
		}
	case float64:
		// Assume Unix timestamp in UTC
		inputTime = time.Unix(int64(v), 0).UTC()
	case int64:
		inputTime = time.Unix(v, 0).UTC()
	default:
		return nil, fmt.Errorf("cannot parse date from %T", value)
	}

	return t.formatTime(inputTime, t.OutputFormat)
}

func (t *DateTransform) parseTime(value string, format string) (time.Time, error) {
	switch format {
	case "RFC3339":
		return time.Parse(time.RFC3339, value)
	case "RFC3339Nano":
		return time.Parse(time.RFC3339Nano, value)
	case "DateTime":
		return time.ParseInLocation("2006-01-02 15:04:05", value, time.UTC)
	case "Date":
		return time.ParseInLocation("2006-01-02", value, time.UTC)
	default:
		return time.ParseInLocation(format, value, time.UTC)
	}
}

func (t *DateTransform) formatTime(tm time.Time, format string) (string, error) {
	switch format {
	case "RFC3339":
		return tm.Format(time.RFC3339), nil
	case "RFC3339Nano":
		return tm.Format(time.RFC3339Nano), nil
	case "DateTime":
		return tm.Format("2006-01-02 15:04:05"), nil
	case "Date":
		return tm.Format("2006-01-02"), nil
	case "Unix":
		return strconv.FormatInt(tm.Unix(), 10), nil
	case "UnixMilli":
		return strconv.FormatInt(tm.UnixMilli(), 10), nil
	default:
		// Try custom format
		return tm.Format(format), nil
	}
}

// SplitTransform splits a string into an array
type SplitTransform struct {
	Delimiter string
}

func splitTransformCreator(config map[string]interface{}) (Transformer, error) {
	t := &SplitTransform{
		Delimiter: ",", // default
	}

	if delim, ok := config["delimiter"].(string); ok {
		t.Delimiter = delim
	}

	return t, nil
}

func (t *SplitTransform) Transform(value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("split transform requires string input, got %T", value)
	}

	return strings.Split(str, t.Delimiter), nil
}

// JoinTransform joins an array into a string
type JoinTransform struct {
	Delimiter string
}

func joinTransformCreator(config map[string]interface{}) (Transformer, error) {
	t := &JoinTransform{
		Delimiter: ",", // default
	}

	if delim, ok := config["delimiter"].(string); ok {
		t.Delimiter = delim
	}

	return t, nil
}

func (t *JoinTransform) Transform(value interface{}) (interface{}, error) {
	arr, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("join transform requires array input, got %T", value)
	}

	strs := make([]string, len(arr))
	for i, v := range arr {
		strs[i] = fmt.Sprintf("%v", v)
	}

	return strings.Join(strs, t.Delimiter), nil
}

// UpperTransform converts strings to uppercase
type UpperTransform struct{}

func upperTransformCreator(config map[string]interface{}) (Transformer, error) {
	return &UpperTransform{}, nil
}

func (t *UpperTransform) Transform(value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("upper transform requires string input, got %T", value)
	}
	return strings.ToUpper(str), nil
}

// LowerTransform converts strings to lowercase
type LowerTransform struct{}

func lowerTransformCreator(config map[string]interface{}) (Transformer, error) {
	return &LowerTransform{}, nil
}

func (t *LowerTransform) Transform(value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("lower transform requires string input, got %T", value)
	}
	return strings.ToLower(str), nil
}

// TrimTransform trims whitespace from strings
type TrimTransform struct{}

func trimTransformCreator(config map[string]interface{}) (Transformer, error) {
	return &TrimTransform{}, nil
}

func (t *TrimTransform) Transform(value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("trim transform requires string input, got %T", value)
	}
	return strings.TrimSpace(str), nil
}

// ChainTransform applies multiple transforms in sequence
type ChainTransform struct {
	transforms []Transformer
}

// NewChainTransform creates a transform that applies multiple transforms in order
func NewChainTransform(transforms ...Transformer) *ChainTransform {
	return &ChainTransform{transforms: transforms}
}

func (t *ChainTransform) Transform(value interface{}) (interface{}, error) {
	result := value
	for _, transform := range t.transforms {
		var err error
		result, err = transform.Transform(result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// DefaultRegistry is the global transformer registry
var DefaultRegistry = NewRegistry()
