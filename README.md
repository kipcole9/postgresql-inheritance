# Postgresql::Inheritance

Allows inherited tables to be used in Postgresql plus some other goodies.  More documentation to come.  This is *not* ready for prime time.

## Installation

Add this line to your application's Gemfile:

    gem 'postgresql-inheritance'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install postgresql-inheritance

## Usage

Usage is modelled on Posgresql inherited tables.  This is not a general purpose feature of Postgresql but is has
its uses.

### Migrations

In your migrations define a table to inherit from another table:

```ruby
class CreateThings < ActiveRecord::Migration
  def change
    # Things is the head of or inheritance tree representing all things
    # both tangible and intangible.  Can be considered the vertices in
    # the graph.
    create_table :things do |t|
      t.string      :name,              :limit => 50
      t.string      :slug,              :limit => 50
      t.text        :description

      t.timestamps
    end

    create_table :accounts, inherits: :things do |t|
	  # Table :parties inherits attributes of :things above
	  t.string		:subdomain
	
    end
  end
end
```
	
### Schema.rb

A schema will be created that reflects the inheritance chain so that rake:db:schema:load will work

```ruby
ActiveRecord::Schema.define(version: 20140215022916) do
  create_table "things", id: :uuid, default: "uuid_generate_v4()", force: true do |t|
    t.string   "name",             limit: 50
    t.string   "slug",             limit: 50
    t.text     "description"
  end

  create_table "accounts", inherits: "things", schema: "public" do |t|
    t.string "subdomain"
  end
end
```
	
### In your application code

ActiveRecord queries work as usual with the following differences:

* When retrieving records from a parent, any records that actually below to a subclass are coerced to that class.
By default only those columns in the parent class are returned (this avoids multiple database queries).  To return
all columns for all classes then call `reload_subclasses` in the returned ActiveRecord::Relation

* The default query of "*" is changed to include the OID of each row so we can do class discrimination.  The default `select will be "things"."tableoid"::regclass as "type", "things".*`

### Other features

MultitableInheritance also includes support for other Postgresql features:

* Schemas
* Enums

## Contributing

1. Fork it ( https://github.com/[my-github-username]/postgresql-inheritance/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
