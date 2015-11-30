module ActiveRecord
  module QueryMethods

  private
    # Retrieve the OID as well on a default select
    def build_select(arel)
      if select_values.any?
        arel.project(*arel_columns(select_values.uniq))
      else
        arel.project("\"#{klass.table_name}\".\"tableoid\"::regclass as \"type\"") if @klass.using_multi_table_inheritance?
        arel.project(@klass.arel_table[Arel.star])
      end
    end
  end
end