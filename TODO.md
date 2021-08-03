ToDo
====

Tasks
-----

* Make link names configurable
* Make property names configurable
* Use version number from pipeline
* Allow event offset to be reset?
* Handle dead letters?

Notes
-----

a page with no events will not have a next link
a page with no events will have an empty array for embedded events?
a page with no events will not have an embedded events key?
any page will have a templatable "since" link expecting a event ID
any page will have a self link including any query params
a page where there are further pages of events will have a next link
  encoding the correct query params
termination logic for a given poll is that either next link missing or
  embedded events is {missing|empty}
alternatively, use max events per poll as page size and use ID of last event
  on page as since offset