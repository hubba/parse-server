// const mongodb = require('mongodb');
// const Collection = mongodb.Collection;

export default class MongooseCollection {
  _mongooseCollection;

  constructor(mongooseCollection) {
    this._mongooseCollection = mongooseCollection;
  }

  // Does a find with "smart indexing".
  // Currently this just means, if it needs a geoindex and there is
  // none, then build the geoindex.
  // This could be improved a lot but it's not clear if that's a good
  // idea. Or even if this behavior is a good idea.
  find(query, { skip, limit, sort, keys, maxTimeMS } = {}) {
    return this._rawFind(query, { skip, limit, sort, keys, maxTimeMS })
      .catch(error => {
        // Check for "no geoindex" error
        if (error.code != 17007 && !error.message.match(/unable to find index for .geoNear/)) {
          throw error;
        }
        // Figure out what key needs an index
        const key = error.message.match(/field=([A-Za-z_0-9]+) /)[1];
        if (!key) {
          throw error;
        }

        var index = {};
        index[key] = '2d';
        return this._mongooseCollection.createIndex(index)
          // Retry, but just once.
          .then(() => this._rawFind(query, { skip, limit, sort, keys, maxTimeMS }));
      });
  }

  _rawFind(query, { skip, limit, sort, keys, maxTimeMS } = {}) {
    console.log({'this._mongooseCollection': this._mongooseCollection})
    return new Promise((resolve, reject) => {
      console.log({'this._mongooseCollection': this._mongooseCollection})
      this._mongooseCollection.find(query).exec((err, objects) => {

      if (!!err) {
        return reject(err);
      }

      return resolve(objects);
      // if (keys) {
      //   findOperation = findOperation.project(keys);
      // }
      //
      // if (maxTimeMS) {
      //   findOperation = findOperation.maxTimeMS(maxTimeMS);
      // }

      // return findOperation.toArray();
      });
    });
  }

  count(query, { skip, limit, sort, maxTimeMS } = {}) {
    // const countOperation = this._mongooseCollection.count(query, { skip, limit, sort, maxTimeMS });
    //
    // return countOperation;

    console.log({'this._mongooseCollection': this._mongooseCollection})
    return new Promise((resolve, reject) => {
      console.log({'this._mongooseCollection': this._mongooseCollection})
      this._mongooseCollection.count(query).exec((err, objects) => {

      if (!!err) {
        return reject(err);
      }

      return resolve(objects);
      // if (keys) {
      //   findOperation = findOperation.project(keys);
      // }
      //
      // if (maxTimeMS) {
      //   findOperation = findOperation.maxTimeMS(maxTimeMS);
      // }

      // return findOperation.toArray();
      });
    });
  }

  insertOne(object) {
    return this._mongooseCollection.insertOne(object);
  }

  // Atomically updates data in the database for a single (first) object that matched the query
  // If there is nothing that matches the query - does insert
  // Postgres Note: `INSERT ... ON CONFLICT UPDATE` that is available since 9.5.
  upsertOne(query, update) {
    return this._mongooseCollection.update(query, update, { upsert: true })
  }

  updateOne(query, update) {
    return this._mongooseCollection.updateOne(query, update);
  }

  updateMany(query, update) {
    return this._mongooseCollection.updateMany(query, update);
  }

  deleteOne(query) {
    return this._mongooseCollection.deleteOne(query);
  }

  deleteMany(query) {
    return this._mongooseCollection.deleteMany(query);
  }

  findAndModify(query, update) {
    console.log('findAndModify')
    console.log({query: query})
    console.log({update: update})
    return this._mongooseCollection.findAndModify(query, [], update, { new: true });
  }

  _ensureSparseUniqueIndexInBackground(indexRequest) {
    return new Promise((resolve, reject) => {
      this._mongooseCollection.ensureIndex(indexRequest, { unique: true, background: true, sparse: true }, (error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  drop() {
    return this._mongooseCollection.drop();
  }
}
