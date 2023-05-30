// SPDX-License-Identifier: GPL-2.0-or-later

package clients

import (
	"fmt"
	"regexp"

	log "github.com/sirupsen/logrus"
)

type Cmder interface {
	GetCommand() []string
	ExtractResult(string) (map[string]string, error)
}

type Cmd struct {
	key         string
	prefix      []string
	suffix      []string
	cmd         []string
	cleanupFunc func(string) string
	regex       *regexp.Regexp
	fullCmd     []string
}

func NewCmd(key string, cmd []string) (*Cmd, error) {
	cmdInstance := Cmd{
		key:    key,
		cmd:    cmd,
		prefix: []string{"echo", fmt.Sprintf("'<%s>'", key)},
		suffix: []string{"echo", fmt.Sprintf("'</%s>'", key)},
	}

	cmdInstance.fullCmd = make([]string, 0)
	cmdInstance.fullCmd = append(cmdInstance.fullCmd, cmdInstance.prefix...)
	cmdInstance.fullCmd = append(cmdInstance.fullCmd, ";")
	cmdInstance.fullCmd = append(cmdInstance.fullCmd, cmdInstance.cmd...)

	lastPart := cmdInstance.cmd[len(cmdInstance.cmd)-1]
	if string(lastPart[len(lastPart)-1]) != ";" {
		cmdInstance.fullCmd = append(cmdInstance.fullCmd, ";")
	}

	cmdInstance.fullCmd = append(cmdInstance.fullCmd, cmdInstance.suffix...)
	cmdInstance.fullCmd = append(cmdInstance.fullCmd, ";")

	compiledRegex, err := regexp.Compile(`(?s)<` + key + `>\n` + `(.*)` + `\n</` + key + `>`)
	if err != nil {
		return nil, fmt.Errorf("failed to complie regex for key %s: %w", key, err)
	}
	cmdInstance.regex = compiledRegex
	return &cmdInstance, nil
}

func (c *Cmd) SetCleanupFunc(f func(string) string) {
	c.cleanupFunc = f
}

func (c *Cmd) GetCommand() []string {
	return c.fullCmd
}

func (c *Cmd) ExtractResult(s string) (map[string]string, error) {
	result := make(map[string]string)
	log.Debugf("extract %s from %s", c.key, s)
	match := c.regex.FindStringSubmatch(s)
	log.Debugf("match %#v", match)

	if len(match) > 0 {
		value := match[1]

		if c.cleanupFunc != nil {
			value = c.cleanupFunc(match[1])
		}
		log.Debugf("r %s", value)
		result[c.key] = value
		return result, nil
	}
	return result, fmt.Errorf("failed to find result for key: %s", c.key)
}

type CmdGroup struct {
	cmds []*Cmd
}

func (cgrp *CmdGroup) AddCommand(c *Cmd) {
	cgrp.cmds = append(cgrp.cmds, c)
}

func (cgrp *CmdGroup) GetCommand() []string {
	res := make([]string, 0)
	for _, c := range cgrp.cmds {
		res = append(res, c.GetCommand()...)
		lastPart := res[len(res)-1]
		if string(lastPart[len(lastPart)-1]) != ";" {
			res = append(res, ";")
		}
	}
	return res
}

func (cgrp *CmdGroup) ExtractResult(s string) (map[string]string, error) {
	results := make(map[string]string)
	for _, c := range cgrp.cmds {
		res, err := c.ExtractResult(s)
		if err != nil {
			return results, err
		}
		results[c.key] = res[c.key]
	}
	return results, nil
}
