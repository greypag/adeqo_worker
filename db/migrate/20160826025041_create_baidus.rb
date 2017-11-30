class CreateBaidus < ActiveRecord::Migration
  def change
    create_table :baidus do |t|

      t.timestamps
    end
  end
end
