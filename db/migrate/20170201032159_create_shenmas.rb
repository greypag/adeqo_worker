class CreateShenmas < ActiveRecord::Migration
  def change
    create_table :shenmas do |t|

      t.timestamps
    end
  end
end
