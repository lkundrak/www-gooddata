package WWW::GoodData::Agent;

=head1 NAME

WWW::GoodData::Agent - HTTP client for GoodData JSON-based API

=head1 SYNOPSIS

  use WWW::GoodData::Agent;
  my $ua = new WWW::GoodData::Agent;
  my $metadata = $ua->get ('/md');

=head1 DESCRIPTION

B<WWW::GoodData::Agent> is HTTP user agent that makes it easy for follow
specifics of the GoodData service API, transparently handles conversion
to and from JSON content type and recognizes and handles various kinds
of exceptions and error states.

It is a subclass of L<LWP::UserAgent> and follows its semantics unless
documented otherwise.

=cut

use strict;
use warnings;

use base qw/LWP::UserAgent/;
use JSON::XS;

our $VERSION = '1.0';

=head1 PROPERTIES

=over 4

=item root

L<URI> object pointing to root of the service API.

This is used to resolve relative request paths.

=back

=head1 METHODS

=over 4

=item new ROOT, PARAMS

Creates a new agent instance. First argument is root of
the service API, the rest is passed to L<LWP::UserAgent> as is.

Compared to stock L<LWP::UserAgent>, it has a memory-backed
cookie storage and sets the B<Accept> header to prefer JSON content.

=cut

sub new
{
	my ($self, $root, @args) = @_;
	$self = $self->SUPER::new (@args);
	$self->{root} = $root;
	$self->agent ("perl-WWW-GoodData/$VERSION ");
	# Not backed by a file yet
	$self->cookie_jar ({});
	# Prefer JSON, but deal with whatever else comes in, instead of letting backend return 406s
	# XML is to make Backend think we're a browser, and redirect us upon token expirations
	$self->default_header (Accept =>
		'application/json;q=0.9, text/plain;q=0.2, application/xhtml+xml;q=0.1, */*;q=0.1');
	return $self;
}

=item post URI, BODY, PARAMS

Constructs and issues a POST request.

Compared to stock L<LWP::UserAgent>, the extra body parameter
is encoded into JSON and set as request content, which is the
only way to set the request content.

The rest of parameters are passed to L<LWP::UserAgent> untouched.

=cut

sub post
{
	my ($self, $uri, $body, @args) = @_;
	push @args,'Content-Type' => 'application/json',
		Content => encode_json ($body);
	return $self->SUPER::post ($uri, @args);
}

=item request PARAMS

This call is common for all request types.

While API is same as stock L<LWP::UserAgent>, relative URIs
are permitted and extra content processing is done with the response.

Namely, errors are either handled or turned into exceptions
and known content types (JSON) are decoded.

=cut

sub request
{
	my ($self, $request, @args) = @_;

	# URI relative to root
	$request->uri ($request->uri->abs ($self->{root}));

	# Issue the request
	my $response = $self->SUPER::request ($request, @args);

	# Pass processed response from subrequest (redirect)
	return $response if ref $response eq 'HASH';

	# Decode
	my $decoded = $response->header ('Content-Type') eq 'application/json'
		? decode_json ($response->content)
		: { raw => $response->content };

	# Error handling
	unless ($response->is_success) {
		# Apache::Error exceptions lack error wrapper
		$decoded = $decoded->{error} if exists $decoded->{error};
		die $response->status_line unless exists $decoded->{message};
		die sprintf $decoded->{message}, @{$decoded->{parameters}};
	}

	return $decoded;
}

=back

=head1 SEE ALSO

=over

=item *

L<https://secure.gooddata.com/gdc/> -- Browsable GoodData API

=item *

L<LWP::UserAgent> -- Perl HTTP client
        
=back

=head1 COPYRIGHT

Copyright 2011, Lubomir Rintel

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=head1 AUTHOR

Lubomir Rintel C<lkundrak@v3.sk>

=cut

1;
