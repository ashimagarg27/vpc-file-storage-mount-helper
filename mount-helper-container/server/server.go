/**
 *
 * Copyright 2024- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strings"
	"syscall"

	mounterUtils "github.com/IBM/ibm-object-csi-driver/pkg/mounter/utils"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	_ = flag.Set("logtostderr", "true") // #nosec G104: Attempt to set flags for logging to stderr only on best-effort basis.Error cannot be usefully handled.
	logger = setUpLogger()
	defer logger.Sync()
}

var (
	logger     *zap.Logger
	socketDir  = "/var/lib/"
	socketPath = socketDir + "ibmshare.sock"

	s3fs           = "s3fs"
	metaRoot       = "/var/lib/ibmc-s3fs"
	passFile       = ".passwd-s3fs" // #nosec G101: not password
	metaRootRclone = "/var/lib/ibmc-rclone"
	configPath     = "/root/.config/rclone"
	configFileName = "rclone.conf"
	remote         = "ibmcos"
	s3Type         = "s3"
	cosProvider    = "IBMCOS"
	envAuth        = "true"
)

// SystemOperation is an interface for system operations like mount and unmount.
type SystemOperation interface {
	Execute(command string, args ...string) (string, error)
}

// RealSystemOperation is an implementation of SystemOperation that performs actual system operations.
type RealSystemOperation struct{}

func (rs *RealSystemOperation) Execute(command string, args ...string) (string, error) {
	cmd := exec.Command(command, args...)

	output, err := cmd.CombinedOutput()
	return string(output), err
}

func setUpLogger() *zap.Logger {
	// Prepare a new logger
	atom := zap.NewAtomicLevel()
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		atom,
	), zap.AddCaller()).With(zap.String("ServiceName", "mount-helper-conatiner-service"))
	atom.SetLevel(zap.InfoLevel)
	return logger
}

func main() {
	// Always create fresh socket file
	os.Remove(socketPath)

	// Create a listener
	logger.Info("Creating unix socket listener...")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		logger.Fatal("Failed to create unix socket listener:", zap.Error(err))
	}
	// Close the listener at the end
	defer listener.Close()

	// Handle SIGINT and SIGTERM signals to gracefully shut down the server
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		os.Remove(socketPath)
		os.Exit(0)
	}()

	logger.Info("Starting mount-helper-container service...")

	// Create gin router
	router := gin.Default()

	sysOp := &RealSystemOperation{}

	// Add REST APIs to router
	router.POST("/api/mount", handleMounting(sysOp))
	router.POST("/api/umount", handleUnMount(sysOp))
	router.GET("/api/mountHelperContainerStatus", mountHelperContainerStatus)
	router.POST("/api/debugLogs", debugLogs(sysOp))
	router.GET("/api/mountStatus", mountStatus(sysOp))

	router.POST("/api/cos/mount", handleCosMount())
	router.POST("/api/cos/unmount", handleCosUnmount())

	// Serve HTTP requests over Unix socket
	err = http.Serve(listener, router)
	if err != nil {
		logger.Fatal("Error while serving HTTP requests:", zap.Error(err))
	}
}

func mountHelperContainerStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"Message": "Mount-helper-container server is live!"})
}

// handleMounting mounts ibmshare based file system mountPath to targetPath
func handleMounting(sysOp SystemOperation) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request struct {
			MountPath  string `json:"mountPath"`
			TargetPath string `json:"targetPath"`
			FsType     string `json:"fsType"`
			RequestID  string `json:"requestID"`
		}

		if err := c.BindJSON(&request); err != nil {
			logger.Error("Invalid request: ", zap.Error(err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
			return
		}

		logger.Info("New mount request with values: ", zap.String("RequestID:", request.RequestID), zap.String("Source mount Path:", request.MountPath), zap.String("Target Path:", request.TargetPath))

		// execute mount command
		options := "mount -t " + request.FsType + " -o secure=true " + request.MountPath + " " + request.TargetPath + " -v"

		logger.Info("Command to execute is: ", zap.String("Command:", options))

		output, err := sysOp.Execute("mount", "-t", request.FsType, "-o", "secure=true", request.MountPath, request.TargetPath, "-v")
		if err != nil {
			logger.Error("Mounting failed with error: ", zap.Error(err))
			logger.Error("Command output: ", zap.String("output", output))
			response := gin.H{
				"MountExitCode": err.Error(),
				"Description":   output,
			}
			c.JSON(http.StatusInternalServerError, response)
			return
		}

		logger.Info("Command output: ", zap.String("", output))
		c.JSON(http.StatusOK, gin.H{"message": "Request processed successfully"})
	}
}

// handleUnMount does umount on a targetPath provided
func handleUnMount(sysOp SystemOperation) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request struct {
			TargetPath string `json:"targetPath"`
		}

		if err := c.BindJSON(&request); err != nil {
			logger.Error("Invalid request: ", zap.Error(err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
			return
		}

		logger.Info("New umount request with values: ", zap.String("Target Path:", request.TargetPath))

		output, err := sysOp.Execute("umount", request.TargetPath)

		if err != nil {
			logger.Error("Umount failed with error: ", zap.Error(err))
			logger.Error("Command output: ", zap.String("output", output))
			response := gin.H{
				"MountExitCode": err.Error(),
				"Description":   output,
			}
			c.JSON(http.StatusInternalServerError, response)
			return
		}

		c.JSON(http.StatusOK, gin.H{"Message": "Request processed successfully"})
	}
}

// debugLogs collectes logs necessary in case there are any mount failures.
func debugLogs(sysOp SystemOperation) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request struct {
			RequestID string `json:"requestID"`
		}

		if err := c.BindJSON(&request); err != nil {
			logger.Error("Invalid request: ", zap.Error(err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
			return
		}

		output, err := sysOp.Execute("journalctl", "-u", "mount-helper-container")

		if err != nil {
			logger.Error("Unable to fetch logs, error: ", zap.Error(err))
			logger.Error("Command output: ", zap.String("output", output))
			response := gin.H{
				"MountExitCode": err.Error(),
				"Description":   output,
			}
			c.JSON(http.StatusInternalServerError, response)
			return
		}

		logFile, err := os.Create("/tmp/mount-helper-container.log")
		if err != nil {
			logger.Error("Not able to create log file: ", zap.Error(err))
			return
		}
		defer logFile.Close()

		// Write the output to the file
		_, err = logFile.WriteString(string(output))
		if err != nil {
			logger.Error("Error writing to file:", zap.Error(err))
			return
		}

		// mount-helper logs are stored at /opt/ibm/mount-ibmshare/mount-ibmshare.log. Available at volume mount path

		c.JSON(http.StatusOK, gin.H{"Message": "Request processed successfully"})
	}
}

// mountStatus takes target directory as input and checks if it is a valid mount directory
func mountStatus(sysOp SystemOperation) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request struct {
			TargetPath string `json:"targetPath"`
		}

		if err := c.BindJSON(&request); err != nil {
			logger.Error("Invalid request: ", zap.Error(err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
			return
		}

		logger.Info("New find mount request with values: ", zap.String("Target Path:", request.TargetPath))

		output, err := sysOp.Execute("findmnt", request.TargetPath)

		if err != nil {
			logger.Error("'findmnt' failed with error: ", zap.Error(err))
			response := gin.H{
				"MountExitCode": err.Error(),
				"Description":   output,
			}
			c.JSON(http.StatusInternalServerError, response)
			return
		}

		c.JSON(http.StatusOK, "Success!!")
	}
}

func handleCosMount() gin.HandlerFunc {
	return func(c *gin.Context) {
		var request struct {
			Path      string   `json:"path"`
			Command   string   `json:"command"`
			Args      []string `json:"args"`
			APIKey    string   `json:"apiKey"`
			AccessKey string   `json:"accessKey"`
			SecretKey string   `json:"secretKey"`
		}

		logger.Info("New mount request with values: ", zap.Any("Request:", request))

		if err := c.BindJSON(&request); err != nil {
			logger.Error("Invalid request: ", zap.Error(err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
			return
		}

		logger.Info("New mount request with values: ", zap.String("Path:", request.Path), zap.String("Command:", request.Command), zap.Any("Args:", request.Args))

		var pathExist bool
		var err error

		if request.Command == s3fs {
			metaPath := path.Join(metaRoot, fmt.Sprintf("%x", sha256.Sum256([]byte(request.Path))))
			if pathExist, err = checkPath(metaPath); err != nil {
				logger.Error("S3FSMounter Mount: Cannot stat directory", zap.String("metaPath", metaPath), zap.Error(err))
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
				return
			}

			logger.Info("metaPath", zap.String("", metaPath))

			if !pathExist {
				if err = mkdirAll(metaPath, 0755); // #nosec G301: used for s3fs
				err != nil {
					logger.Error("S3FSMounter Mount: Cannot create directory", zap.String("metaPath", metaPath), zap.Error(err))
					c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
					return
				}
			}

			logger.Info("path created")

			accessKeys := ""
			if request.APIKey != "" {
				accessKeys = fmt.Sprintf(":%s", request.APIKey)
			} else {
				accessKeys = fmt.Sprintf("%s:%s", request.AccessKey, request.SecretKey)
			}

			passwdFile := path.Join(metaPath, passFile)
			if err = writePassWrap(passwdFile, accessKeys); err != nil {
				logger.Error("S3FSMounter Mount: Cannot create file", zap.String("metaPath", metaPath), zap.Error(err))
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
				return
			}

			logger.Info("pass file", zap.String("passwdFile", passwdFile))
		} else {
			metaPath := path.Join(metaRootRclone, fmt.Sprintf("%x", sha256.Sum256([]byte(request.Path))))
			if pathExist, err = checkPath(metaPath); err != nil {
				logger.Error("RcloneMounter Mount: Cannot stat directory", zap.String("metaPath", metaPath), zap.Error(err))
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
				return
			}

			if !pathExist {
				if err = mkdirAll(metaPath, 0755); // #nosec G301: used for rclone
				err != nil {
					logger.Error("RcloneMounter Mount: Cannot create directory", zap.String("metaPath", metaPath), zap.Error(err))
					c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
					return
				}
			}

			accessKeys := fmt.Sprintf("%s:%s", request.AccessKey, request.SecretKey)

			configPathWithVolID := path.Join(configPath, fmt.Sprintf("%x", sha256.Sum256([]byte(request.Path))))
			if err = createConfigWrap(configPathWithVolID, accessKeys); err != nil {
				logger.Error("RcloneMounter Mount: Cannot create rclone config file %v", zap.String("metaPath", metaPath), zap.Error(err))
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
				return
			}
		}

		utils := mounterUtils.MounterOptsUtils{}
		err = utils.FuseMount(request.Path, request.Command, request.Args)
		if err != nil {
			logger.Error("Mount Failed: ", zap.Error(err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Mount Failed"})
			return
		}

		c.JSON(http.StatusOK, "Success!!")
	}
}

func handleCosUnmount() gin.HandlerFunc {
	return func(c *gin.Context) {
		var request struct {
			Path string `json:"path"`
		}

		if err := c.BindJSON(&request); err != nil {
			logger.Error("Invalid request: ", zap.Error(err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
			return
		}

		logger.Info("New unmount request with values: ", zap.String("Path:", request.Path))

		utils := mounterUtils.MounterOptsUtils{}
		err := utils.FuseUnmount(request.Path)
		if err != nil {
			logger.Error("Mount Failed: ", zap.Error(err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Mount Failed"})
			return
		}

		c.JSON(http.StatusOK, "Success!!")
	}
}

func checkPath(path string) (bool, error) {
	if path == "" {
		return false, errors.New("undefined path")
	}
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else if isCorruptedMnt(err) {
		return true, err
	}
	return false, err
}

func isCorruptedMnt(err error) bool {
	if err == nil {
		return false
	}
	var underlyingError error
	switch pe := err.(type) {
	case *os.PathError:
		underlyingError = pe.Err
	case *os.LinkError:
		underlyingError = pe.Err
	case *os.SyscallError:
		underlyingError = pe.Err
	}
	return underlyingError == syscall.ENOTCONN || underlyingError == syscall.ESTALE
}

var mkdirAllFunc = os.MkdirAll

// Function that wraps os.MkdirAll
var mkdirAll = func(path string, perm os.FileMode) error {
	return mkdirAllFunc(path, perm)
}

var writePassFunc = writePass

// Function that wraps writePass
var writePassWrap = func(pwFileName string, pwFileContent string) error {
	return writePassFunc(pwFileName, pwFileContent)
}

func writePass(pwFileName string, pwFileContent string) error {
	pwFile, err := os.OpenFile(pwFileName, os.O_RDWR|os.O_CREATE, 0600) // #nosec G304: Value is dynamic
	if err != nil {
		return err
	}
	_, err = pwFile.WriteString(pwFileContent)
	if err != nil {
		return err
	}
	err = pwFile.Close() // #nosec G304: Value is dynamic
	if err != nil {
		return err
	}
	return nil
}

var createConfigFunc = createConfig

// Function that wraps writePass
var createConfigWrap = func(configPathWithVolID, accessKeys string) error {
	return createConfigFunc(configPathWithVolID, accessKeys)
}

func createConfig(configPathWithVolID, accessKeys string) error {
	var accessKey string
	var secretKey string
	keys := strings.Split(accessKeys, ":")
	if len(keys) == 2 {
		accessKey = keys[0]
		secretKey = keys[1]
	}
	configParams := []string{
		"[" + remote + "]",
		"type = " + s3Type,
		"endpoint = " + "https://s3.direct.au-syd.cloud-object-storage.appdomain.cloud", // FIX
		"provider = " + cosProvider,
		"env_auth = " + envAuth,
		"location_constraint = " + "au-syd-standard", // FIX
		"access_key_id = " + accessKey,
		"secret_access_key = " + secretKey,
	}

	// configParams = append(configParams, rclone.MountOptions...)  // FIX
	configParams = append(configParams, "acl=private", "bucket_acl=private", "upload_cutoff=256Mi", "chunk_size=64Mi",
		"max_upload_parts=64", "upload_concurrency=20", "copy_cutoff=1Gi", "memory_pool_flush_time=30s", "disable_checksum=true")

	if err := os.MkdirAll(configPathWithVolID, 0755); // #nosec G301: used for rclone
	err != nil {
		logger.Error("RcloneMounter Mount: Cannot create directory", zap.String("configPathWithVolID", configPathWithVolID), zap.Error(err))
		return err
	}

	configFile := path.Join(configPathWithVolID, configFileName)
	file, err := os.Create(configFile) // #nosec G304 used for rclone
	if err != nil {
		logger.Error("RcloneMounter Mount: Cannot create file", zap.String("configFileName", configFileName), zap.Error(err))
		return err
	}
	defer func() {
		if err = file.Close(); err != nil {
			logger.Error("RcloneMounter Mount: Cannot close file", zap.String("configFileName", configFileName), zap.Error(err))
		}
	}()

	err = os.Chmod(configFile, 0644) // #nosec G302: used for rclone
	if err != nil {
		logger.Error("RcloneMounter Mount: Cannot change permissions on file", zap.String("configFileName", configFileName), zap.Error(err))
		return err
	}

	logger.Info("-Rclone writing to config-")
	datawriter := bufio.NewWriter(file)
	for _, line := range configParams {
		_, err = datawriter.WriteString(line + "\n")
		if err != nil {
			logger.Error("RcloneMounter Mount: Could not write to config file: %v", zap.Error(err))
			return err
		}
	}
	err = datawriter.Flush()
	if err != nil {
		return err
	}
	logger.Info("-Rclone created rclone config file-")
	return nil
}
