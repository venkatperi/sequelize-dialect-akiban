var Query = require( "./query" );
var Utils = require( "sequelize" ).Utils;
var Dialects = require( "sequelize" ).Dialects;
var Akiban = require( 'akiban-node' );
var Q = require( 'q' );

var ConnectorManager = function ( sequelize, options ) {
  this.sequelize = sequelize;
  this.options = options;
  this.version = new Akiban.Version( options );
  this.client = new Akiban.Sql( options );
  this.connecting = false;
  this.connected = Q.defer();
  this.connect();
};

Utils._.extend( ConnectorManager.prototype, Dialects.prototype );
var p = ConnectorManager.prototype;

p.connect = function () {
  var self = this;

  this.version.version().then(
      function ( result ) {
        self.version = result;
        self.connecting = false;
        self.connected.resolve( self.client );
      },
      function ( err ) {
        self.connecting = false;
        self.error = err;
        self.client = null;
        self.connected.reject( err );
      } ).done();
};

p.query = function ( sql, callee, options ) {
  var self = this;
  var query = new Query( self.client, self.sequelize, callee, options );
  this.connected.promise.then(
      function ( client ) {
        query.run( sql );
      },
      function ( err ) {

      } ).done();

  return query;
};

ConnectorManager.Query = Query;
ConnectorManager.QueryGenerator = require( './query-generator' );
module.exports = ConnectorManager;

