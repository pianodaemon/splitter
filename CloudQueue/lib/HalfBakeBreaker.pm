package HalfBakeBreaker;

use strict;
use warnings;
use Scalar::Util qw(blessed);
use Time::HiRes qw(gettimeofday);

our $VERSION = '0.01';

# It stands for a simple boolean definition.
use constant {
    true  => 1,
    false => 0,
};

# It enables debugging verbosity
our $debug = false;

sub new {

   my $class = shift;
   my $self = {};

   # This variable let us turn on the debug mode
   $debug = $ENV{HALF_BAKE_BREAKER_DEBUG} if (exists($ENV{HALF_BAKE_BREAKER_DEBUG}));

   # pick up all the key value arguments
   my %kvargs = @_;
   $self->{$_} = $kvargs{$_} for (keys %kvargs);

   $self->{_current_retries_number} = 0;
   $self->{_circuit_open_until} = 0;

   unless (exists $self->{max_retries_number}) {
     die "For constructor the max_retries_number is an integer mandatory key value argument";
   }

   unless (exists $self->{open_time}) {
     die "For constructor the open_time is an integer (secs) mandatory key value argument";
   }

   unless (exists $self->{on_circuit_close}) {
     die "For constructor the on_circuit_close handler is a mandatory key value argument";
   }  

   unless (exists $self->{on_circuit_open}) {
     die "For constructor the on_circuit_open handler is a mandatory key value argument";
   }

   unless (exists $self->{error_if_code}) {
      die "For constructor the error_if_code handler is a mandatory key value argument";
   }

   bless $self, $class;
   return $self;
}

sub engage {
  my ($self, $attempt_code) = @_;

  if (my $timestamp = $self->{_circuit_open_until}) {
    # we can't execute until the timestamp has done
    my ($seconds, $microseconds) = gettimeofday;
    $seconds * 1000 + int($microseconds / 1000) >= $timestamp
      or die 'The circuit is now open and cannot be executed.';
    $self->{_circuit_open_until} = 0;
    $self->{on_circuit_close}();
  }

  my $error;
  my @attempt_result;
  my $attempt_result;
  my $wantarray;

  if (wantarray) {
    $wantarray = 1;
    @attempt_result = eval { $attempt_code->(@_) };
    $error = $@;
  } elsif ( ! defined wantarray ) {
    eval { $attempt_code->(@_) };
    $error = $@;
  } else {
    $attempt_result = eval { $attempt_code->(@_) };
    $error = $@;
  }

  my $h = {
    action_retry => $self,
    attempt_result => ( $wantarray ? \@attempt_result : $attempt_result ),
    attempt_parameters => \@_,
  };

  if ($self->{error_if_code}($error, $h)) {
    $self->{_current_retries_number} = $self->{_current_retries_number} + 1;
    if ($self->{_current_retries_number} >= $self->{max_retries_number}) {
      my ($seconds, $microseconds) = gettimeofday;
      my $open_until = ($self->{open_time} * 1000) + ($seconds * 1000 + int($microseconds / 1000));
      $self->{_circuit_open_until} = $open_until;
      $self->{on_circuit_open}();
    }
    die $error;
  } else {
    return $h->{attempt_result};
  }
}

true;
