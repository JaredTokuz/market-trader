package stocks

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Repository interface allows us to access the CRUD Operations in mongo here.
type Repository interface {
	FindOne(collectionName string, filter interface{}, opts *options.FindOneOptions) (*interface{}, error)
	BulkWrite(collectionName string, operations []mongo.WriteModel, opts *options.BulkWriteOptions) (error)
	UpdateOne(collectionName string, filter interface{}, update interface{}, opts *options.UpdateOptions) (error)
}
type repository struct {
	Database *mongo.Database
}

//NewRepo is the single instance repo that is being created.
func NewRepo(database *mongo.Database) Repository {
	return &repository{
		Database: database,
	}
}

func (r *repository) FindOne(collectionName string, filter interface{}, opts *options.FindOneOptions) (*interface{}, error) {
	result := r.Database.Collection(collectionName).FindOne(context.TODO(), filter, opts)
	var data interface{}
	err := result.Err()
	if err != nil {
		return nil, err
	}
	_ = result.Decode(&data)
	return &data, nil
}

func (r *repository) BulkWrite(collectionName string, operations []mongo.WriteModel, opts *options.BulkWriteOptions) (error) {	
	result, err := r.Database.Collection(collectionName).BulkWrite(context.TODO(), operations, opts)
	if err != nil {
		return err
	}
	log.Println(result)
	return nil
}

func (r *repository) UpdateOne(collectionName string, filter interface{}, update interface{}, opts *options.UpdateOptions) (error) {	
	result, err := r.Database.Collection(collectionName).UpdateOne(context.TODO(), filter, update, opts)
	if err != nil {
		return err
	}
	log.Println(result)
	return nil
}
