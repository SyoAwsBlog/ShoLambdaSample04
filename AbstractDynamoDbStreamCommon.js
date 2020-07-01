/*
業務毎（機能枚）に共通処理な処理などを、この層に実装する。

例えば
・API Gateway から呼び出すLambdaに共通
・SQSを呼び出すLambdaに共通
など、用途毎に共通となるような処理は、この層に実装すると良い

written by syo
http://awsblog.physalisgp02.com
*/
module.exports = function AbstractDynamoDbStreamCommon() {
  // 疑似的な継承関係として親モジュールを読み込む
  var superClazzFunc = require("./AbstractBizCommon");
  // prototypeにセットする事で継承関係のように挙動させる
  AbstractDynamoDbStreamCommon.prototype = new superClazzFunc();

  // ワーカー処理同時実行数
  var EXECUTORS_THREADS_NUM = "1";
  if (process && process.env && process.env.ExecutorsThreadsNum) {
    EXECUTORS_THREADS_NUM = process.env.ExecutorsThreadsNum;
  }

  // ワーカー処理トランザクション制御（１周毎にwaitする：ミリ秒指定）
  var EXECUTORS_THREADS_WAIT = "0";
  if (process && process.env && process.env.ExecutorsThreadsWait) {
    EXECUTORS_THREADS_WAIT = process.env.ExecutorsThreadsWait;
  }

  // 処理の実行
  function* executeBizUnitCommon(event, context, bizRequireObjects) {
    var base = AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# executeBizUnitCommon : start"
      );

      // 自動再実行ようの演算
      var reCallCount = event.reCallCount || 0;
      reCallCount += 1;
      event.reCallCount = reCallCount;

      // 読み込みモジュールの引き渡し
      AbstractDynamoDbStreamCommon.prototype.RequireObjects = bizRequireObjects;

      // 親の業務処理を実行
      return yield AbstractDynamoDbStreamCommon.prototype.executeBizCommon(
        event,
        context,
        bizRequireObjects
      );
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# executeBizUnitCommon : end"
      );
    }
  }
  AbstractDynamoDbStreamCommon.prototype.executeBizUnitCommon = executeBizUnitCommon;

  AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon.getExecutorsThreadsNum = function () {
    var base = AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# getExecutorsThreadsNum : start"
      );
      return parseInt(EXECUTORS_THREADS_NUM);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# getExecutorsThreadsNum : end"
      );
    }
  }.bind(AbstractDynamoDbStreamCommon); // AbstractDynamoDbStreamCommonをthisとする

  AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon.getExecutorsThreadsWait = function () {
    var base = AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# getExecutorsThreadsWait : start"
      );
      return parseInt(EXECUTORS_THREADS_WAIT);
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# getExecutorsThreadsWait : end"
      );
    }
  }.bind(AbstractDynamoDbStreamCommon); // AbstractDynamoDbStreamCommonをthisとする

  /*
  業務前処理（オーバーライドのサンプル）

  API Gatewayからの呼び出しなど、Lambda実行引数を、後続処理で呼び出しやすくする為に、
  初期処理で返却しておく事で、getFirstIndexObjectで取得できるようになる。

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon.beforeMainExecute = function (
    args
  ) {
    var base = AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# beforeMainExecute : start"
      );

      // tasksの先頭には event が格納されてくる。
      // 後続のtaskで参照しやすくするには、最初のタスクで
      // eventを返却しておくと良い
      var event = args;

      return new Promise(function (resolve, reject) {
        if (event && event.Records) {
          resolve(event.Records);
        } else {
          reject("event.Records Not Exists");
        }
      });

      return event;
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# beforeMainExecute : end"
      );
    }
  }.bind(AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon);

  /*
  業務メイン処理

  N件あるデータをワーカー処理（疑似スレッド処理）に引き渡し実行制御をする。

  @override
  @param args 各処理の結果を格納した配列
  */
  AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon.businessMainExecute = function (
    args
  ) {
    var base = AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# businessMainExecute : start"
      );

      // 直前のPromise処理の結果取得
      var beforePromiseResult = base.getLastIndexObject(args);

      //　ワーカー処理のクラス取得
      var subWorkerClazzFunc = base.getSubWorkerClazzFunc();

      var datas = beforePromiseResult || [];
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# DynamoStream RecordCount:" + datas.length
      );

      return new Promise(function (resolve, reject) {
        var executorsErrArray = [];
        var executorsResultArray = [];

        function sleep(msec, val) {
          return new Promise(function (resolve, reject) {
            setTimeout(resolve, msec, val);
          });
        }

        function* sub(record, threadsWait) {
          try {
            var workerFuncObj = new subWorkerClazzFunc();

            var result = yield workerFuncObj.execute(
              record,
              base.promiseRefs.context,
              base.RequireObjects
            );
            executorsResultArray.push(result);

            if (threadsWait > 0) {
              base.writeLogTrace(
                "AbstractDynamoDbStreamCommon# businessMainExecute : worker sleep:" +
                  String(threadsWait)
              );
              yield sleep(threadsWait, "Sleep Wait");
            }
          } catch (catchErr) {
            if (threadsWait > 0) {
              base.writeLogTrace(
                "AbstractDynamoDbStreamCommon# businessMainExecute : worker sleep:" +
                  String(threadsWait)
              );
              yield sleep(threadsWait, "Sleep Wait");
            }
            executorsErrArray.push(catchErr);
          }
        }

        function* controller(datas) {
          var results = [];
          try {
            var threadsNum = base.getExecutorsThreadsNum();
            var threadsWait = base.getExecutorsThreadsWait();
            var executors = base.RequireObjects.Executors(threadsNum);

            for (var i = 0; i < datas.length; i++) {
              var record = datas[i];
              results.push(executors(sub, record, threadsWait));
            }

            // Worker処理（疑似スレッド）の待ち合わせ
            yield results;

            // エラーハンドリング
            if (executorsErrArray.length > 0) {
              var irregularErr;
              for (var i = 0; i < executorsErrArray.length; i++) {
                var executorsErr = executorsErrArray[i];
                base.printStackTrace(executorsErr);

                if (i == 0) {
                  irregularErr = executorsErr;
                }

                if (!base.judgeBizError(executorsErr)) {
                  irregularErr = executorsErr;
                }
              }
              if (irregularErr) {
                throw irregularErr;
              }
            }

            return executorsResultArray;
          } catch (catchErr) {
            yield results;
            throw catchErr;
          }
        }

        base.RequireObjects.aa(controller(datas))
          .then(function (results) {
            resolve(results);
          })
          .catch(function (err) {
            reject(err);
          });
      });
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# businessMainExecute : end"
      );
    }
  }.bind(AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon);

  /*
  ワーカー処理（疑似スレッド処理）のクラスを返却（オーバーライド必須）
  */
  AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon.getSubWorkerClazzFunc = function () {
    var base = AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon;
    try {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# getSubWorkerClazzFunc : start"
      );
      return require("./AbstractWorkerSnsCallTopicCommon.js");
    } catch (err) {
      base.printStackTrace(err);
      throw err;
    } finally {
      base.writeLogTrace(
        "AbstractDynamoDbStreamCommon# getSubWorkerClazzFunc : end"
      );
    }
  };

  return {
    executeBizUnitCommon,
    AbstractDynamoDbStreamCommon,
    AbstractBizCommon: AbstractDynamoDbStreamCommon.prototype,
    AbstractBaseCommon:
      AbstractDynamoDbStreamCommon.prototype.AbstractBaseCommon,
  };
};
