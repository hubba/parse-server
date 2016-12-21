'use strict';


module.exports.register = (mongoose) => {

  console.log('winning')

  var Schema = mongoose.Schema,
    JobStatusSchema;

  JobStatusSchema = new Schema({
     "jobName":    {type: 'String'},
     "source":     {type: 'String'},
     "status":     {type: 'String'},
     "message":    {type: 'String'},
     "params":     {type: 'Object'}, // params received when calling the job
     "finishedAt": {type: 'Date'}
   });

  mongoose.model( '_JobStatus', JobStatusSchema );

  console.log('won')
}
