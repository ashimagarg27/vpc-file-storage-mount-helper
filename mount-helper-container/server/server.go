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
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	csiConfig "github.com/IBM/ibm-object-csi-driver/config"
	"github.com/IBM/ibm-object-csi-driver/pkg/driver"
	"github.com/IBM/ibm-object-csi-driver/pkg/mounter"
	mounterUtils "github.com/IBM/ibm-object-csi-driver/pkg/mounter/utils"
	"github.com/IBM/ibm-object-csi-driver/pkg/s3client"
	"github.com/IBM/ibm-object-csi-driver/pkg/utils"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
		secretName := c.Query("secret")
		secretNamespace := c.Query("secretNamespace")
		storageClassName := c.Query("storageClass")
		// pvcName := c.Query("pvc")
		// pvcNamespace := c.Query("pvcNamespace")

		os.Setenv("KUBERNETES_SERVICE_HOST", "172.21.0.1")
		os.Setenv("KUBERNETES_SERVICE_PORT", "443")

		k8sClient, err := createK8sClient()
		if err != nil {
			response := gin.H{
				"MountExitCode": err.Error(),
				"Description":   err.Error(),
			}
			c.JSON(http.StatusInternalServerError, response)
			return
		}

		_, err = k8sClient.StorageV1().StorageClasses().Get(context.TODO(), storageClassName, metav1.GetOptions{})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{})
			return
		}
		// storageClassMountFlags := storageClass.MountOptions
		// storageClassParameters := storageClass.Parameters

		// Fetch PV for given PVC
		pvs, err := k8sClient.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{})
			return
		}

		var requiredPVs []corev1.PersistentVolume
		for ind := range pvs.Items {
			if pvs.Items[ind].Spec.StorageClassName == storageClassName {
				requiredPVs = append(requiredPVs, pvs.Items[ind])
			}
		}

		if len(requiredPVs) == 0 || len(requiredPVs) > 1 { // check for all PVs
			c.JSON(http.StatusInternalServerError, gin.H{})
			return
		}

		volumeID := requiredPVs[0].Name
		volumeAttributes := requiredPVs[0].Spec.PersistentVolumeSource.CSI.VolumeAttributes

		var requiredSecret corev1.Secret
		var foundSecret bool

		secrets, err := k8sClient.CoreV1().Secrets("").List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{})
			return
		}

		if secretNamespace == "" {
			secretNamespace = "default"
		}

		for ind := range secrets.Items {
			if secrets.Items[ind].Name == secretName && secrets.Items[ind].Namespace == secretNamespace {
				requiredSecret = secrets.Items[ind]
				foundSecret = true
				break
			}
		}

		if !foundSecret {
			c.JSON(http.StatusInternalServerError, gin.H{})
			return
		}

		// secretsForReq := map[string]string{}
		// volCtxForReq := map[string]string{}

		// for k, v := range requiredSecret.Data {
		// 	if k == "accessKey" || k == "secretKey" || k == "apiKey" || k == "kpRootKeyCRN" {
		// 		secretsForReq[k] = string(v)
		// 	} else {
		// 		volCtxForReq[k] = string(v)
		// 	}
		// }

		// mountFlags := storageClassMountFlags
		// attr := volumeAttributes
		// secret, err := decodeSecretData(requiredSecret.Data)
		// if err != nil {
		// 	c.JSON(http.StatusInternalServerError, gin.H{})
		// 	return
		// }
		// m := objcsi.NewCSIMounterFactory()
		// mounter := m.NewMounter(attr, secret, mountFlags)
		// if err := mounter.Mount("", targetPath); err != nil {
		// 	c.JSON(http.StatusInternalServerError, gin.H{})
		// 	return
		// }

		//   csi.storage.k8s.io/ephemeral:false csi.storage.k8s.io/pod.name:cos-csi-app csi.storage.k8s.io/pod.namespace:default csi.storage.k8s.io/pod.uid:c56ad355-e92e-4d21-a690-33b9d4fc5ee6   csi.storage.k8s.io/serviceAccount.name:default

		options := getOptions()

		csiDriver, err := driver.Setups3Driver(options.ServerMode, csiConfig.CSIDriverName, csiConfig.VendorVersion, logger)
		if err != nil {
			logger.Fatal("Failed to setup s3 driver", zap.Error(err))
			os.Exit(1)
		}

		statsUtil := &(utils.DriverStatsUtils{})
		mounterUtil := &(mounterUtils.MounterOptsUtils{})
		mountObj := mounter.NewCSIMounterFactory()

		S3CSIDriver, err := csiDriver.NewS3CosDriver(options.NodeID, options.Endpoint, s3client.NewObjectStorageSessionFactory(), mountObj, statsUtil, mounterUtil)
		if err != nil {
			logger.Fatal("Failed in initialize s3 COS driver", zap.Error(err))
			os.Exit(1)
		}

		nodeServer := driver.NodeServer{
			S3Driver:     S3CSIDriver,
			Stats:        statsUtil,
			NodeID:       options.NodeID,
			Mounter:      mountObj,
			MounterUtils: mounterUtil,
		}
		req := &csi.NodePublishVolumeRequest{
			VolumeId:         volumeID,
			TargetPath:       "", // target_path:"/var/data/kubelet/pods/c56ad355-e92e-4d21-a690-33b9d4fc5ee6/volumes/kubernetes.io~csi/pvc-38bc6f35-99a3-47c4-acd8-8fa577b24c5f/mount"
			VolumeCapability: &csi.VolumeCapability{},
			Secrets:          requiredSecret.StringData,
			VolumeContext:    volumeAttributes,
		}

		//  volume_capability:<mount:<mount_flags:"multipart_size=62" mount_flags:"max_dirty_data=51200" mount_flags:"parallel_count=8" mount_flags:"max_stat_cache_size=100000" mount_flags:"retries=5" mount_flags:"kernel_cache" > access_mode:<mode:SINGLE_NODE_WRITER > >

		//  volume_context:<key:"csi.storage.k8s.io/ephemeral" value:"false" >
		// volume_context:<key:"csi.storage.k8s.io/pod.name" value:"cos-csi-app" >
		// volume_context:<key:"csi.storage.k8s.io/pod.namespace" value:"default" >
		// volume_context:<key:"csi.storage.k8s.io/pod.uid" value:"c56ad355-e92e-4d21-a690-33b9d4fc5ee6" >
		//  volume_context:<key:"csi.storage.k8s.io/serviceAccount.name" value:"default" >

		resp, err := nodeServer.NodePublishVolume(context.TODO(), req)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{})
			return
		}

		fmt.Println("RESPONSE", resp)
		c.JSON(http.StatusOK, "Success!!")
	}
}

func handleCosUnmount() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, "Success!!")
	}
}

func createK8sClient() (*kubernetes.Clientset, error) {
	// Create a Kubernetes client configuration
	config, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("Error creating Kubernetes client configuration: ", zap.Error(err))
		return nil, err
	}

	// Create a Kubernetes clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Error("Error creating Kubernetes clientset: ", zap.Error(err))
		return nil, err
	}

	return clientset, nil
}

// func decodeSecretData(data map[string][]byte) (map[string]string, error) {
// 	decoded := make(map[string]string)
// 	for key, value := range data {
// 		decodedValue, err := base64.StdEncoding.DecodeString(string(value))
// 		if err != nil {
// 			return nil, fmt.Errorf("error decoding key %s: %v", key, err)
// 		}
// 		decoded[key] = string(decodedValue)
// 	}
// 	return decoded, nil
// }

type Options struct {
	ServerMode     string
	Endpoint       string
	NodeID         string
	MetricsAddress string
}

func getOptions() *Options {
	var (
		endpoint       = flag.String("endpoint", "unix:/tmp/csi.sock", "CSI endpoint")
		serverMode     = flag.String("servermode", "controller", "Server Mode node/controller")
		nodeID         = flag.String("nodeid", "host01", "node id")
		metricsAddress = flag.String("metrics-address", "0.0.0.0:9080", "Metrics address")
	)
	_ = flag.Set("logtostderr", "true") // #nosec G104: Attempt to set flags for logging to stderr only on best-effort basis.Error cannot be usefully handled.
	flag.Parse()
	return &Options{
		ServerMode:     *serverMode,
		Endpoint:       *endpoint,
		NodeID:         *nodeID,
		MetricsAddress: *metricsAddress,
	}
}
