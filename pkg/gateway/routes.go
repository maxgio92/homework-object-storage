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
	ErrObjectIDMissing  = errors.New("object id missing")
	ErrObjectIDNotValid = errors.New("object id not valid")
	ErrNodePoolEmpty    = errors.New("node pool empty")
	ErrClientBuild      = errors.New("error building client")
	ErrReadingBody      = errors.New("error reading body")
)

func (g *Gateway) AddObjectRoutes(r *mux.Router) {
	objectRouter := r.PathPrefix("/object").Subrouter()
	objectRouter.Methods(http.MethodGet).Path(fmt.Sprintf("/{id:%s}", objectIDRegex)).HandlerFunc(g.GetObjectHandler)
	objectRouter.Methods(http.MethodPut).Path(fmt.Sprintf("/{id:%s}", objectIDRegex)).HandlerFunc(g.PutObjectHandler)
}

func (g *Gateway) HomeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "MinIO object storage gateway\n")
}

func (g *Gateway) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	objectID := vars["id"]
	if objectID == "" {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrObjectIDMissing.Error())
		return
	}
	if len(objectID) > maxObjectIDsize {
		g.logger.Debug("requested object id is not valid")

		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrObjectIDNotValid.Error())
		return
	}

	if g.nodePool == nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrNodePoolEmpty.Error())
		return
	}

	nodeID := g.nodePool.ObjectToNodeID(objectID)
	if nodeID == "" {
		nodeID = g.nodePool.Next()
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

	obj, err := client.GetObject(r.Context(), bucket, objectID, minio.GetObjectOptions{})
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
		WithField("object id", objectID).
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

	objectID := vars["id"]
	if objectID == "" {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrObjectIDMissing.Error())
		return
	}
	if len(objectID) > maxObjectIDsize {
		g.logger.Debug("requested object id is not valid")

		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrObjectIDNotValid.Error())
		return
	}

	if g.nodePool == nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrNodePoolEmpty.Error())
		return
	}

	nodeID := g.nodePool.ObjectToNodeID(objectID)
	if nodeID == "" {
		nodeID = g.nodePool.Next()
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

	upload, err := client.PutObject(r.Context(), bucket, objectID, buf, nRead,
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(err.Error())
		return
	}

	g.nodePool.SetObjectNodeID(objectID, nodeID)

	g.logger.
		WithField("operation", http.MethodPut).
		WithField("object id", upload.Key).
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
