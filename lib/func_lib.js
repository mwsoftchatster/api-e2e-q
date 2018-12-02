/* jshint esnext: true */
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-e2e-q/config/config.js');
var email = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-e2e-q/lib/email_lib.js');


/**
 *  Setup the pool of connections to the db so that every connection can be reused upon it's release
 *
 */
var mysql = require('mysql');
var Sequelize = require('sequelize');
const sequelize = new Sequelize(config.db.name, config.db.user_name, config.db.password, {
    host: config.db.host,
    dialect: config.db.dialect,
    port: config.db.port,
    operatorsAliases: config.db.operatorsAliases,
    pool: {
      max: config.db.pool.max,
      min: config.db.pool.min,
      acquire: config.db.pool.acquire,
      idle: config.db.pool.idle
    }
});


/**
 *  Publishes message on api-e2e-c topic with newly generated one time keys batch
 */
function publishOnE2EC(amqpConn, message, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiE2EC.*';
            var toipcName = `apiE2EC.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, toipcName, new Buffer(message));
        });
    }else {
        // log and send error
        email.sendApiE2EQErrorEmail('API E2E C publishOnE2EC AMPQ connection was null');
    }
}

/**
 * Model of user_one_time_pre_key_pair table
 * 
 */
const OneTimePreKey = sequelize.define('user_one_time_pre_key_pair', {
    user_id: { 
            type: Sequelize.INTEGER,
            allowNull: false
        },
    one_time_pre_key_pair_pbk: {type: Sequelize.BLOB('long'), allowNull: false},
    one_time_pre_key_pair_uuid: {type: Sequelize.STRING, allowNull: false}
}, {
  freezeTableName: true, // Model tableName will be the same as the model name
  timestamps: false,
  underscored: true
});


/**
 *  Saves public one time keys into db received from api-user-c during registration
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.saveRegistrationPublicKeys = function(message, amqpConn, topic){
    OneTimePreKey.bulkCreate(message.oneTimePreKeyPairPbks, { fields: ['user_id','one_time_pre_key_pair_pbk', 'one_time_pre_key_pair_uuid'] }).then(() => {
        var response = {
            status: config.rabbitmq.statuses.ok
        };

        // publish to api-e2e-q
        publishOnE2EC(amqpConn, JSON.stringify(response), topic);
    }).error(function(err){
        var response = {
            status: config.rabbitmq.statuses.error
        };

        // publish to api-e2e-c
        publishOnE2EC(amqpConn, JSON.stringify(response), topic);
        email.sendApiE2EQErrorEmail(err);
    });
};


/**
 *  Saves public one time keys into db
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.saveReRegistrationPublicKeys = function(message, amqpConn, topic){
    sequelize.query('CALL DeleteOldOneTimePublicKeysByUserId(?)',
    { replacements: [ message.oneTimePreKeyPairPbks[0].user_id ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            OneTimePreKey.bulkCreate(message.oneTimePreKeyPairPbks, { fields: ['user_id','one_time_pre_key_pair_pbk', 'one_time_pre_key_pair_uuid'] }).then(() => {
                var response = {
                    status: config.rabbitmq.statuses.ok
                };
        
                // publish to api-e2e-c
                publishOnE2EC(amqpConn, JSON.stringify(response), topic);
              }).error(function(err){
                var response = {
                    status: config.rabbitmq.statuses.error
                };
        
                // publish to api-e2e-c
                publishOnE2EC(amqpConn, JSON.stringify(response), topic);
                email.sendApiE2EQErrorEmail(err);
            });
    }).error(function(err){
        var response = {
            status: config.rabbitmq.statuses.error
        };

        // publish to api-e2e-c
        publishOnE2EC(amqpConn, JSON.stringify(response), topic);
        email.sendApiE2EQErrorEmail(err);
    });
};


/**
 *  Checks if users public keys need to be replenished
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.checkPublicKeys = function(req,res){
    sequelize.query('CALL CheckIfNewPublicKeysNeeded(?)',
    { replacements: [ req.query.userId ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            if(result[0].keys_left > 100) {
                res.json("no");
            }else{
                res.json("yes");
            } 
    }).error(function(err){
        email.sendApiE2EQErrorEmail(err);
        res.json("error");
    });
};


/**
 *  Fetches one one time public key
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.getOneTimePublicKey = function(req,res){
    var oneTimePublicKey = {
        userId: req.query.contactId,
        oneTimePublicKey: null,
        uuid: null
    };

    sequelize.query(
        "select * from `user_one_time_pre_key_pair` where user_id=" + req.query.contactId + " limit 1",
        { type: sequelize.QueryTypes.SELECT})
        .then(function(result) {
            oneTimePublicKey.oneTimePublicKey = Array.prototype.slice.call(result[0].one_time_pre_key_pair_pbk, 0);
            oneTimePublicKey.uuid = result[0].one_time_pre_key_pair_uuid;
            res.json(oneTimePublicKey);
    });
};


/**
 *  Fetches one one time public key by UUID
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.getOneTimePublicKeyByUUID = function(req,res){
    var oneTimePublicKey = {
        userId: req.query.contactId,
        oneTimePublicKey: null,
        uuid: null
    };

    sequelize.query(
        "select * from `user_one_time_pre_key_pair` where user_id=" + req.query.contactId + " and one_time_pre_key_pair_uuid = '" + req.query.uuid + "'",
        { type: sequelize.QueryTypes.SELECT})
        .then(function(result) {
            oneTimePublicKey.oneTimePublicKey = Array.prototype.slice.call(result[0].one_time_pre_key_pair_pbk, 0);
            oneTimePublicKey.uuid = result[0].one_time_pre_key_pair_uuid;
            res.json(oneTimePublicKey);
    });
};


/**
 *  Removes public one time keys from db after processing message
 * 
 * (contactPublicKeyUUID UUID): UUID of one time public key that was used to process one message
 * (userPublicKeyUUID UUID): UUID of one time public key that was used to process one message
 * (amqpConn Object): RabbitMQ object that was 
 */
module.exports.deleteOneTimePublicKeysByUUID = function(message, amqpConn, topic){
    console.log("deleteOneTimePublicKeysByUUID has been called");
    sequelize.query('CALL DeleteOneTimePublicKeysByUUID(?,?)',
    { replacements: [ message.contactPublicKeyUUID, message.userPublicKeyUUID ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            var response = {
                status: config.rabbitmq.statuses.ok
            };
    
            // publish to api-e2e-c
            publishOnE2EC(amqpConn, JSON.stringify(response), topic);
    }).error(function(err){
        var response = {
            status: config.rabbitmq.statuses.error
        };

        // publish to api-e2e-c
        publishOnE2EC(amqpConn, JSON.stringify(response), topic);
    });
};