package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7"
)

var (
	ErrObjectKeyMissing  = errors.New("object key missing")
	ErrObjectKeyNotValid = errors.New("object key not valid")
	ErrNodePoolEmpty     = errors.New("node pool empty")
	ErrClientBuild       = errors.New("error building client")
	ErrReadingBody       = errors.New("error reading body")
)

func (g *Gateway) AddObjectRoutes(r *mux.Router) {
	objectRouter := r.PathPrefix("/object").Subrouter()
	objectRouter.Methods(http.MethodGet).Path(fmt.Sprintf("/{key:%s}", objectKeyRegex)).HandlerFunc(g.GetObjectHandler)
	objectRouter.Methods(http.MethodPut).Path(fmt.Sprintf("/{id:%s}", objectKeyRegex)).HandlerFunc(g.PutObjectHandler)
}

func (g *Gateway) HomeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "MinIO object storage gateway\n")
}

func (g *Gateway) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	objectKey := vars["key"]
	if objectKey == "" {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrObjectKeyMissing.Error())
		return
	}
	if len(objectKey) > maxObjectKeysize {
		g.logger.Debug("requested object key is not valid")

		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrObjectKeyNotValid.Error())
		return
	}

	if g.nodePool == nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrNodePoolEmpty.Error())
		return
	}

	nodeID := g.nodePool.ObjectToNodeID(objectKey)
	if nodeID == "" {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrNodePoolEmpty.Error())
		return
	}

	client := g.nodePool.NodeClient(nodeID)

	if client == nil {
		g.logger.Debug("node client is nil")

		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrClientBuild.Error())
		return
	}

	bucket := defaultBucket
	region := defaultRegion
	if err := g.ensureBucket(r.Context(), client, bucket, region); err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(err.Error())
		return
	}

	obj, err := client.GetObject(r.Context(), bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(err.Error())
		return
	}
	defer obj.Close()

	content, err := io.ReadAll(obj)
	if err != nil {
		if minio.ToErrorResponse(err).StatusCode == http.StatusNotFound {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(err.Error())
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(err.Error())
		return
	}
	g.logger.
		WithField("operation", http.MethodGet).
		WithField("object key", objectKey).
		WithField("node id", nodeID).
		Info("request")

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(content)
}

func (g *Gateway) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	buf := &bytes.Buffer{}
	nRead, err := io.Copy(buf, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrReadingBody.Error())
		return
	}

	objectKey := vars["id"]
	if objectKey == "" {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrObjectKeyMissing.Error())
		return
	}
	if len(objectKey) > maxObjectKeysize {
		g.logger.Debug("requested object key is not valid")

		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrObjectKeyNotValid.Error())
		return
	}

	if g.nodePool == nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrNodePoolEmpty.Error())
		return
	}

	nodeID := g.nodePool.ObjectToNodeID(objectKey)
	if nodeID == "" {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrNodePoolEmpty.Error())
		return
	}
	if nodeID == "" {
		g.logger.Debug("node id is empty")

		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrNodePoolEmpty.Error())
		return
	}

	client := g.nodePool.NodeClient(nodeID)
	if client == nil {
		g.logger.Debug("node client is nil")

		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrClientBuild.Error())
		return
	}

	bucket := defaultBucket
	region := defaultRegion
	if err = g.ensureBucket(r.Context(), client, bucket, region); err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(err.Error())
		return
	}

	upload, err := client.PutObject(r.Context(), bucket, objectKey, buf, nRead,
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(err.Error())
		return
	}

	g.logger.
		WithField("operation", http.MethodPut).
		WithField("object key", upload.Key).
		WithField("node id", nodeID).
		Info("request")

	w.WriteHeader(http.StatusOK)
	w.Header().Set(http.CanonicalHeaderKey("Content-Length"), strconv.FormatInt(upload.Size, 10))
	json.NewEncoder(w).Encode(upload)
}

func (g *Gateway) ensureBucket(ctx context.Context, client *minio.Client, name, region string) error {
	exists, err := client.BucketExists(ctx, name)
	if err != nil {
		return err
	}

	if !exists {
		err = client.MakeBucket(context.Background(), name, minio.MakeBucketOptions{Region: region})
		if err != nil {
			return err
		}
	}

	return nil
}
