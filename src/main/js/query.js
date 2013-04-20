var Utils = require( "sequelize" ).Utils
    , AbstractQuery = require( 'sequelize' ).Dialects.abstract.query;

module.exports = (function () {
  var Query = function ( client, sequelize, callee, options ) {
    this.client = client;
    this.sequelize = sequelize;
    this.callee = callee;
    this.options = Utils._.extend( {
      logging : console.log,
      plain : false,
      raw : false
    }, options || {} );

    this.checkLoggingOption();
  };

  Utils.inherit( Query, AbstractQuery );

  Query.prototype.run = function ( sql ) {
    this.sql = sql;

    if ( this.options.logging !== false ) {
      this.options.logging( 'Executing: ' + this.sql )
    }

    var receivedError = false;
    var self = this;

    this.client.query( sql ).then(
        function ( result ) {
          console.log( result );
          self.emit( 'sql', self.sql );
          onSuccess.call( self, result );
        },
        function ( err ) {
          console.log( err );
          receivedError = true;
          self.emit( 'error', err, self.callee );
        }
    );

    return this;
  };

  Query.prototype.getInsertIdField = function () {
    return 'id';
  };

  var onSuccess = function ( rows ) {
    var results = []
        , isTableNameQuery = (this.sql.indexOf( 'SELECT table_name FROM information_schema.tables' ) === 0)
        , isRelNameQuery = (this.sql.indexOf( 'SELECT relname FROM pg_class WHERE oid IN' ) === 0);

    if ( isTableNameQuery || isRelNameQuery ) {
      if ( isRelNameQuery ) {
        results = rows.map( function ( row ) {
          return {
            name : row.relname,
            tableName : row.relname.split( '_' )[0]
          }
        } )
      }
      else {
        results = rows.map( function ( row ) {
          return Utils._.values( row )
        } )
      }
      return this.emit( 'success', results )
    }

    if ( this.send( 'isSelectQuery' ) ) {
      if ( this.sql.toLowerCase().indexOf( 'select column_name' ) === 0 ) {
        var result = {};

        rows.forEach( function ( _result ) {
          result[_result.Field] = {
            type : _result.Type.toUpperCase(),
            allowNull : (_result.Null === 'YES'),
            defaultValue : _result.Default
          }

          if ( result[_result.Field].type === 'BOOLEAN' ) {
            result[_result.Field].defaultValue = { 'false' : false, 'true' : true }[result[_result.Field].defaultValue]

            if ( result[_result.Field].defaultValue === undefined ) {
              result[_result.Field].defaultValue = null
            }
          }

          if ( typeof result[_result.Field].defaultValue === 'string' ) {
            result[_result.Field].defaultValue = result[_result.Field].defaultValue.replace( /'/g, "" )

            if ( result[_result.Field].defaultValue.indexOf( '::' ) > -1 ) {
              result[_result.Field].defaultValue = result[_result.Field].defaultValue.split( '::' )[0]
            }
          }
        } );

        this.emit( 'success', result )
      }
      else {
        this.emit( 'success', this.send( 'handleSelectQuery', rows ) )
      }
    }
    else if ( this.send( 'isShowOrDescribeQuery' ) ) {
      this.emit( 'success', results )
    }
    else if ( this.send( 'isInsertQuery' ) ) {
      for ( var key in rows[0] ) {
        if ( rows[0].hasOwnProperty( key ) ) {
          this.callee[key] = rows[0][key]
        }
      }

      this.emit( 'success', this.callee )
    }
    else if ( this.send( 'isUpdateQuery' ) ) {
      for ( var key in rows[0] ) {
        if ( rows[0].hasOwnProperty( key ) ) {
          this.callee[key] = rows[0][key]
        }
      }

      this.emit( 'success', this.callee )
    }
    else {
      this.emit( 'success', results )
    }
  };

  return Query;
})();
