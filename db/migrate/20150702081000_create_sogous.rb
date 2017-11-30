class CreateSogous < ActiveRecord::Migration
  def change
    create_table :sogous do |t|

      t.timestamps
    end
  end
end
