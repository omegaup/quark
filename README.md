# quark

This is the omegaUp backend. It's composed of a few components:

* `grader`: Manages the run / submission queue. Does not run anything.
* `runner`: Communicates with `grader` to request new submissions to grade. This compiles and runs the code using the [omegajail sandbox](https://github.com/omegaup/omegajail) against all the inputs, compares the outputs to the expected ones (running a validator against them if needed), and assigns scores to each run.
* `broadcaster`: Broadcasts notifications to users in a contest / course. Things like new runs being graded, scoreboards changing, clarifications, etc.
