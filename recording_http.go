package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

var Recordings *RecordingManager
var RecordingCallbacks *RecordingCallbackRunner

type recordingAPIStatus struct {
	RecordingStatus
	Callback RecordingCallbackStatus `json:"callback"`
}

func recordingResponse(id string, status RecordingStatus, err error) recordingAPIStatus {
	if err != nil {
		status.Error = err.Error()
		if status.State == "" {
			status.State = "error"
		}
	}
	response := recordingAPIStatus{RecordingStatus: status}
	if RecordingCallbacks != nil {
		response.Callback = RecordingCallbacks.Status(id)
	}
	return response
}

func recordingAvailable(c *gin.Context) bool {
	c.Header("Cache-Control", "no-store")
	if !Config.ext(c.Param("uuid")) {
		c.JSON(http.StatusNotFound, ResponseError{"stream not found"})
		return false
	}
	if Recordings == nil {
		c.JSON(http.StatusServiceUnavailable, ResponseError{"recording service unavailable"})
		return false
	}
	return true
}

func HTTPRecordingStatus(c *gin.Context) {
	if !recordingAvailable(c) {
		return
	}
	id := c.Param("uuid")
	c.JSON(http.StatusOK, recordingResponse(id, Recordings.Status(id), nil))
}

func HTTPRecordingStart(c *gin.Context) {
	if !recordingAvailable(c) {
		return
	}
	id := c.Param("uuid")
	status, err := Recordings.Start(id)
	if err != nil {
		c.JSON(http.StatusConflict, recordingResponse(id, status, err))
		return
	}
	c.JSON(http.StatusOK, recordingResponse(id, status, nil))
}

func HTTPRecordingStop(c *gin.Context) {
	if !recordingAvailable(c) {
		return
	}
	id := c.Param("uuid")
	status, err := Recordings.Stop(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, recordingResponse(id, status, err))
		return
	}
	c.JSON(http.StatusOK, recordingResponse(id, status, nil))
}
