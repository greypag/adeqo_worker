require 'test_helper'

class ThreesixtiesControllerTest < ActionController::TestCase
  setup do
    @threesixty = threesixties(:one)
  end

  test "should get index" do
    get :index
    assert_response :success
    assert_not_nil assigns(:threesixties)
  end

  test "should get new" do
    get :new
    assert_response :success
  end

  test "should create threesixty" do
    assert_difference('Threesixty.count') do
      post :create, threesixty: {  }
    end

    assert_redirected_to threesixty_path(assigns(:threesixty))
  end

  test "should show threesixty" do
    get :show, id: @threesixty
    assert_response :success
  end

  test "should get edit" do
    get :edit, id: @threesixty
    assert_response :success
  end

  test "should update threesixty" do
    patch :update, id: @threesixty, threesixty: {  }
    assert_redirected_to threesixty_path(assigns(:threesixty))
  end

  test "should destroy threesixty" do
    assert_difference('Threesixty.count', -1) do
      delete :destroy, id: @threesixty
    end

    assert_redirected_to threesixties_path
  end
end
