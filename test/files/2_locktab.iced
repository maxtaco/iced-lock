
{Table} = require '../../'

exports.locktab_simple = (T,cb) ->
  t = new Table
  for i in [0...10]
    await t.acquire2 { name : "foo" }, defer err, lock, was_open
    T.assert was_open, "was open #{i}"
    T.assert lock?
    T.no_error err
    lock.release()
  cb null

exports.locktab_lock = (T,cb) ->
  state = 0
  tab = new Table

  fn = (err, lock) ->
    T.no_error err
    T.equal(state, 0)
    state++
    tab.acquire2 { name : "bar"} , fn2
    T.equal(state, 1)
    state++
    lock.release()

  fn2 = (err, lock2) ->
    T.no_error err
    T.equal(state, 2)
    state++
    lock2.release()
    cb null

  tab.acquire2 { name: "bar"}, fn

exports.locktab_lock_advisory = (T,cb) ->
  tab = new Table
  tab.acquire2 { name : "foo", no_wait : false}, (err, lock, was_open) ->
    T.assert lock?
    T.assert was_open
    T.no_error err
    tab.acquire2 { name : "foo", no_wait : true }, (err2, lock2, was_open) ->
      T.no_error err2
      T.assert not(lock2?)
      cb null

locktab_lock_stress_one = (T,cb) ->

  tab = new Table

  keys = {}
  n_needed = 0
  for i in [0...100]
    keys[i.toString()] = 0
    n_needed++
  done = 0

  finish = () ->
    T.equal tab.locks.size, 0
    cb()

  test_key = (key) ->
    T.equal keys[key], 0
    keys[key]++
    await setTimeout defer(), Math.random()*5
    tab.acquire2 {name:key}, (err, lock) ->
      T.no_error err
      tab.acquire2 {name:key}, (err2, lock2) ->
        T.no_error err2
        T.equal keys[key], 2
        done++
        lock2.release()
        if done is n_needed then finish()
      T.equal keys[key], 1
      keys[key]++
      await setTimeout defer(), Math.random()*5
      lock.release()

  for k of keys
    test_key k

exports.locktab_lock_stress = (T,cb) ->
  for i in [0...25]
    await locktab_lock_stress_one T, defer()
  cb null

exports.locktab_acquire2_error = (T,cb) ->
  tab = new Table
  await tab.acquire2 {}, defer err, ret
  T.assert not(ret?)
  T.assert err?
  cb null

exports.test_was_open = (T,cb) ->
  tab = new Table
  tab.acquire2 { name : "foo" }, (err, lock, was_open) ->
    T.assert lock?
    T.no_error err
    T.assert was_open
    tab.acquire2 { name : "foo" }, (err, lock, was_open) ->
      T.assert not(was_open)
      T.no_error err
      T.assert lock?
      lock.release()
      cb()
    lock.release()
