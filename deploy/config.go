package deploy

import "github.com/mitchellh/mapstructure"

type SSHHostConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
	User string `mapstructure:"user"`
}

type CustomConfig struct {
	Hosts  []SSHHostConfig `mapstructure:"hosts"`
	SSHKey string          `mapstructure:"sshKey"`
}

// Return Config from stack attributes
func ConfigFromAttributes(attributes map[string]interface{}) (*CustomConfig, error) {
	config := &CustomConfig{}
	// mapstructure is smart enough to decode nested slices of structs
	err := mapstructure.Decode(attributes, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}