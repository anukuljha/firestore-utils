package firestore_utils

import (
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"fmt"
	"github.com/getlantern/errors"
	"google.golang.org/api/option"
	"io/ioutil"
	"os"
)

type Filter func(snapshot *firestore.DocumentSnapshot) bool
type Transform func(data map[string]interface{}) map[string]interface{}

const projectIdEnvName = "GCP_PROJECT_ID"
const credentialEnvName = "GOOGLE_APPLICATION_CREDENTIALS"
const maxAllowedBatchWrite = 499

//CreateFirestoreClientFromConfig creates a firestore client from config
func CreateFirestoreClientFromConfig(ctx context.Context) (*firestore.Client, error) {
	projectId := os.Getenv(projectIdEnvName)
	if projectId == "" {
		exitAndShowRequiredParams(projectIdEnvName)
	}
	cred := os.Getenv(credentialEnvName)
	if cred == "" {
		exitAndShowRequiredParams(credentialEnvName)
	}
	fsClient, err := firestore.NewClient(ctx, projectId, option.WithCredentialsFile(cred))

	if err != nil {
		return nil, err
	}
	return fsClient, nil
}

//WriteCollectionToJSON write a collection as json with same name as collection
func WriteCollectionToJSON(ctx context.Context, collectionName string) (bool, error) {
	fsClient, err := CreateFirestoreClientFromConfig(ctx)
	if err != nil {
		panic(err)
	}
	collectionRef := fsClient.Collection(collectionName)
	readBatchSize := 10000
	totalDocsCount := 0
	docs, err := collectionRef.Limit(readBatchSize).OrderBy("__name__", firestore.Asc).Documents(ctx).GetAll()
	if err != nil {
		return false, err
	}
	result := make([]map[string]interface{}, 0)
	for len(docs) > 0 {
		totalDocsCount += len(docs)
		for _, doc := range docs {
			docData := doc.Data()
			result = append(result, docData)
		}

		docs, err = collectionRef.Limit(readBatchSize).OrderBy("__name__", firestore.Asc).StartAfter(docs[len(docs)-1]).Documents(ctx).GetAll()
		if err != nil {
			return false, err
		}
	}

	fmt.Println("read ", totalDocsCount, "from firestore")
	var filePath = collectionName + ".json"
	writeResult, err := writeToJSON(result, filePath)

	return writeResult, err

}

func writeToJSON(data []map[string]interface{}, filePath string) (bool, error) {

	jsonData, err := json.MarshalIndent(data, "", " ")

	if err != nil {
		print("couldn't marshall to json")
	}

	errWrite := ioutil.WriteFile(filePath, jsonData, 0644)

	if errWrite != nil {
		return false, errWrite
	}

	return true, nil

}

//WriteJSONtoCollection reads a json file and writes to collection
func WriteJSONtoCollection(ctx context.Context, jsonPath string, collectionName string) (bool, error) {
	jsonFile, err := os.Open(jsonPath)

	var result = make([]map[string]interface{}, 0)
	if err != nil {
		fmt.Println("could not open file", err)
		return false, err
	}
	var data []byte
	_, errRead := jsonFile.Read(data)

	if errRead != nil {
		fmt.Println("some error occurred in reading json")
		return false, errRead
	}

	unmarshalErr := json.Unmarshal(data, &result)

	if unmarshalErr != nil {
		fmt.Println("unmarshal error occurred", unmarshalErr)
		return false, unmarshalErr
	}

	return true, nil
}

//WriteToFirestore takes data as map[string]interface{} and writes to Firestore collection
//Will replace any doc if ID in the data already present in Firestore
func WriteToFirestore(ctx context.Context, data []map[string]interface{}, collectionName string, docIDFieldName string) (bool, error) {
	fsClient, err := CreateFirestoreClientFromConfig(ctx)
	if err != nil {
		return false, err
	}

	batch := fsClient.Batch()
	for i, doc := range data {

		docRef := fsClient.Collection(collectionName).NewDoc()
		if docIDFieldName != "" {
			docID, ok := doc[docIDFieldName]

			if !ok {
				return false, errors.New(fmt.Sprintf("No field with name %s present in data for ID", docIDFieldName))
			}
			docIDString, ok := docID.(string)
			if !ok {
				return false, errors.New(fmt.Sprintf("No field with name %s present in data for ID", docIDFieldName))
			}

			docRef = fsClient.Collection(collectionName).Doc(docIDString)
		}
		batch.Set(docRef, doc)
		if i%maxAllowedBatchWrite == 0 {
			_, err := batch.Commit(ctx)
			if err != nil {
				return false, err
			}
			batch = fsClient.Batch()
		}

	}
	//commit remaining docs
	_, err = batch.Commit(ctx)
	if err != nil {
		return false, err
	}
	return true, nil

}

//FilterCollectionDocs applies a Filter func to all the docs and returns the array of filtered ones
func FilterCollectionDocs(ctx context.Context, collectionName string, filterFunc Filter) ([]*firestore.DocumentSnapshot, error) {
	fsClient, err := CreateFirestoreClientFromConfig(ctx)
	if err != nil {
		return nil, err
	}
	collectionRef := fsClient.Collection(collectionName)
	readBatchSize := 10000
	totalDocsCount := 0
	docs, err := collectionRef.Limit(readBatchSize).OrderBy("__name__", firestore.Asc).Documents(ctx).GetAll()
	if err != nil {
		return nil, err
	}
	result := make([]*firestore.DocumentSnapshot, 0)
	for len(docs) > 0 {
		totalDocsCount += len(docs)
		for _, doc := range docs {
			if filterFunc(doc) {
				result = append(result, doc)
			}
		}

		docs, err = collectionRef.Limit(readBatchSize).OrderBy("__name__", firestore.Asc).StartAfter(docs[len(docs)-1]).Documents(ctx).GetAll()
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

//TransformCollectionDocs applies Transform function to each doc of the collection
func TransformCollectionDocs(ctx context.Context, collectionName string, transformFunc Transform) (bool, error) {
	fsClient, err := CreateFirestoreClientFromConfig(ctx)
	if err != nil {
		return false, err
	}
	collectionRef := fsClient.Collection(collectionName)
	readBatchSize := 10000
	totalDocsCount := 0
	docs, err := collectionRef.Limit(readBatchSize).OrderBy("__name__", firestore.Asc).Documents(ctx).GetAll()
	if err != nil {
		return false, err
	}

	for len(docs) > 0 {
		writeBatchCount := 0
		totalDocsCount += len(docs)
		batch := fsClient.Batch()
		for _, doc := range docs {
			docData := doc.Data()
			dataTransformed := transformFunc(docData)
			batch.Set(doc.Ref, dataTransformed)
			writeBatchCount += 1

			if writeBatchCount == maxAllowedBatchWrite {
				_, err = batch.Commit(ctx)

				if err != nil {
					return false, err
				}

				writeBatchCount = 0
				batch = fsClient.Batch()
			}
		}

		if writeBatchCount != 0 {
			_, err := batch.Commit(ctx)

			if err != nil {
				return false, nil
			}

		}

		docs, err = collectionRef.Limit(readBatchSize).OrderBy("__name__", firestore.Asc).StartAfter(docs[len(docs)-1]).Documents(ctx).GetAll()
		if err != nil {
			return false, err
		}
	}

	return true, nil

}

//ApplyToCollection applies Filter function to each doc, if true then applies Transform to the doc
func ApplyToCollection(ctx context.Context, collectionName string, filterFunc Filter, transformFunc Transform) (bool, error) {
	fsClient, err := CreateFirestoreClientFromConfig(ctx)
	if err != nil {
		return false, err
	}
	collectionRef := fsClient.Collection(collectionName)
	readBatchSize := 10000
	totalDocsCount := 0
	docs, err := collectionRef.Limit(readBatchSize).OrderBy("__name__", firestore.Asc).Documents(ctx).GetAll()
	if err != nil {
		return false, err
	}

	for len(docs) > 0 {
		batchCount := 0
		totalDocsCount += len(docs)
		batch := fsClient.Batch()
		for _, doc := range docs {
			if filterFunc(doc) {
				docData := doc.Data()
				dataTransformed := transformFunc(docData)
				batch.Set(doc.Ref, dataTransformed)
				batchCount += 1

			}

			if batchCount == maxAllowedBatchWrite {
				_, err := batch.Commit(ctx)

				if err != nil {
					return false, nil
				}
				batchCount = 0
				batch = fsClient.Batch()
			}
		}

		if batchCount != 0 {
			_, err := batch.Commit(ctx)

			if err != nil {
				return false, nil
			}
		}

		docs, err = collectionRef.Limit(readBatchSize).OrderBy("__name__", firestore.Asc).StartAfter(docs[len(docs)-1]).Documents(ctx).GetAll()
		if err != nil {
			return false, err
		}
	}

	return true, nil

}

func exitAndShowRequiredParams(param string) {
	fmt.Printf("%s is required.\n", param)
	os.Exit(1)
}
